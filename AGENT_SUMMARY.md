# csar-audit Agent Summary

## Role In Prod
Central audit service for the CSAR stack. It ingests audit events, buffers them through RabbitMQ, persists them in PostgreSQL, and exposes an admin query surface through the router.

## Runtime Entry Points
- `cmd/csar-audit/main.go` starts the HTTP ingest/query server, the gRPC ingest server, the consumer loop, and the health sidecar.
- `internal/query/handler.go` serves `GET /admin/audit`.
- `internal/ingest` and `internal/consumer` own ingest buffering and persistence.

## Trust Boundary
- Browser/admin traffic comes through the csar router and uses session auth plus `admin.audit.read`.
- Service ingest comes through the router as `POST /svc/audit/ingest` with STS JWT auth.
- `gatewayctx.TrustedMiddleware` is used in-process, so deployment mTLS and router routing remain part of the trust model.

## Public And Internal Surfaces
- Public/admin surface: `GET /admin/audit`.
- Internal ingest surface: `POST /svc/audit/ingest`.
- Internal gRPC surface: `AuditIngestService` on `:9084`.

## Dependencies
- PostgreSQL for durable audit storage.
- RabbitMQ for ingest buffering and consumer fan-in.
- `csar-core` for config loading, TLS, gateway context, HTTP helpers, health, and observability.
- `csar-proto` for the audit ingest protobuf API.

## Event Identity And Replay
- Ingest prepares owned events with stable UUIDs and timestamps before queueing.
- Transaction-local staging preserves IDs for small/bulk writes; exact replays
  are idempotent, conflicting contents roll back the batch without overwrite.
- The optional gRPC event ID is field 14; deploy upgraded audit instances before
  producer upgrades. ID-less messages already queued have weaker replay guarantees.
- HTTP/gRPC success waits for persistent mandatory publication and broker confirmation.
  PG outages retain unacknowledged batches with backoff; invalid/conflicting events
  require confirmed quarantine before individual ACK. Failed receipts remain uncertain.
- Audit-only unlimited-redelivery policy is verified against RabbitMQ 4.3.6.
  Selected producer outboxes and PG/S3 archive jobs are implemented;
  partitioning and retention remain planned. Production activation is pending;
  verify compatible images, broker policy and pooler before rollout.
- Isolated SQL tests use `AUDIT_TEST_DATABASE_URL`, restricted to a local database
  named `csar_audit_test`. Check TEMP privileges/pooler affinity before deployment.

## Audit Hotspots
- Trust is deployment-sensitive because mTLS and router placement enforce ingress; prod requires client CN `csar-client`.
- gRPC reflection is enabled in prod config and should stay internal-only.
- Ingest buffering and consumer throughput are the main failure domains under load.

## First Files To Read
- `README.md`
- `cmd/csar-audit/main.go`
- `internal/config/config.go`
- `internal/query/handler.go`
- `internal/consumer/runner.go`
- `csar-configs/prod/csar-audit/config.yaml`
- `csar-configs/prod/csar/audit/routes.yaml`
- `csar-configs/prod/csar/audit-svc/routes.yaml`

## DRY / Extraction Candidates
- `internal/rmq/*` and `internal/pipeline/*` are close to the notify service equivalents and are the main cross-repo duplication candidate.
- Keep router client, buffering, and consumer plumbing aligned with `csar-core` primitives rather than growing service-local helpers.

## Required Quality Gates
- `go build ./...`
- `go test ./... -count=1`
- `golangci-lint run ./...`

## Verified logical archive (activation pending, October 8)
- `archive.enabled` is false by default and startup-only. Explicit environment,
  bucket, endpoint, region and existing secret auth are required before enabling.
  The worker uses core S3 streaming/version APIs; it has no deletion path.
- Migration 004 adds nullable canonical `received_at`; historical timestamps
  remain unknown, future inserts default to DB receipt time, exact replay never
  changes the first receipt. The physical event table is not partitioned here.
- A durable pending queue captures committed inserts via a statement trigger,
  including older/COPY writers and late UUIDs; at most200 indexed eligible IDs
  are materialized before payload joins. Known receipts retain one-hour lag.
  A READ COMMITTED locked migration installs capture before a fixed upperUUID;
  restart-safe bootstrap examines at most1000 PK rows before membership joins.
  No steady-state timestamp cursor or full-history anti-join is used. Claims override session isolation to
  READ COMMITTED and are fenced by token/180s lease; work is capped at 150s.
  Batches have at most 200 rows and 32MiB uncompressed content, with conservative
  SQL byte budgeting before fetching event payloads.
- Data upload, full pinned download verification, manifest upload and pinned
  manifest verification precede catalog advancement. Persistent states are
  planned/catalogued; uncertain intermediate IO retains planned membership.
  Conditional writes reuse original versions across retries.
- `Restore` verifies a pinned manifest and complete data before returning any
  records; it neither authorizes a caller nor imports a database. Preserves IDs,
  occurred/receipt timestamps, counts and checksums. Automatic catalog discovery/
  rebuild and authorized cold queries remain unimplemented.
- Lag collectors include unverified jobs and fail visibly on query errors.
  Lag reads32 transactionally maintained full-UUID hash counters plus indexed
  oldest pending entry. Bootstrap-complete distinguishes partial discovery
  from complete historical coverage. Completion fences/catalogues/dequeues in
  one planning-locked transaction. INSERT-only writers use fixed-search-path
  SECURITY DEFINER capture; schema ownership/CREATE restrictions are required.
  All archive workers must upgrade before activation; mixed completers cannot
  maintain the queue. Clone ingest contention/queue churn/capacity remain gates.
  FK membership protects hot rows; it is not sealed-partition retirement proof.
- Local PG 18.6 and fake SDK/IAM object endpoints test crash/lost receipt, lease
  failover, late commits and corruption. Independent copy and production capacity
  remain activation gates. No historical deletion/expiry.

## Archive operator and live preflight (October 8)
- `cmd/csar-audit-archive` has `verify`, `restore-local`, `probe`, `probe-replay`.
  It accepts archive-only configuration and a pinned receipt, enforces byte/time
  bounds, prints metadata only and never starts runtime services or broker clients.
- Imports accept only loopback `csar_audit_restore`, reject fallback hosts, pin
  localhost to 127.0.0.1 and reset schema/options. Verified chunks import atomically
  through existing store staging/deduplication. `RestoreBatch` preserves receipt
  times/NULLs and rejects receipt/content conflicts without overwrite. This tool
  is not an authorized production restore/cutover path.
- Dedicated `aurumskynet-audit-prod` is private, versioned, KMS-encrypted with a
  protected separate key and 64GiB bootstrap cap. Writer verifies versions; reader
  has version reads/listing. Both lack deletion rights; unconditional PUT is denied.
  No lifecycle expiry. Credential state is private and outside source checkouts.
- At 19:36 MSK two synthetic events passed actual Yandex upload, pinned verification,
  conditional replay with unchanged versions, recovery-reader verification and
  two isolated PG 18.6 imports with exactly two rows/IDs and preserved receipt/NULL.
  Objects are retained; no business history was uploaded. Production remains off.
- See `ops/archive/reader.example.yaml`, `bucket-policy.template.json`, README and
  `plans/2026-10-08-audit-s3-preflight.json`. The Docker image packages the CLI alongside the server.
  Compatible core/proto/image releases precede runtime activation.

## Bounded queue and optional replica operator (source follow-up)
- Pending queue uses2% vacuum/analyze factors and1000-row thresholds; clone/infra
  maintenance scheduling remains a gate. Heavy synthetic churn proved index
  cleanup matters even when candidate/metric queries produce zero temp spills.
- `csar-audit-archive replicate` verifies source before writing and destination
  afterward, rewriting destination version receipts while preserving logical
  records/batch/time. It uses archive-only configs and exclusive0600 synced
  receipt files. Same namespace/config/descriptor mismatches are rejected; partial
  uploads are retained for conditional retry using a new local receipt path.
- One existing primary bucket serves all batches. A distinct object namespace
  does not prove an independent failure domain; destination choice and actual
  replication/restore drills remain pending. No new cloud resource or activation.

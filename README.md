# csar-audit

Centralized audit event service for the CSAR ecosystem. Ingests structured
audit events from every microservice, buffers them in RabbitMQ, and persists
them in PostgreSQL with batch writes.

## Architecture

```
                     gRPC (:9084)                          HTTP POST /ingest (:8083)
                         │                                          │
  csar (router) ─────────┤       microservices (via router) ────────┤
                         ▼                                          ▼
                  ┌─────────────────────────────────────────────────────┐
                  │  csar-audit                                         │
                  │                                                     │
                  │  ingest (gRPC + HTTP) ──▶ buffer (chan) ──▶ RabbitMQ│
                  │                                                     │
                  │  consumer (batch) ◀── RabbitMQ ──▶ PostgreSQL       │
                  │                                                     │
                  │  query handler (GET /admin/audit) ◀── PostgreSQL    │
                  └─────────────────────────────────────────────────────┘
```

**Ingest path** — the router calls gRPC `RecordEvents` directly for lowest
latency. Other services use HTTP `POST /svc/audit/ingest` through the router
with STS JWT auth.

**Persistence path** — a batch consumer reads from RabbitMQ, groups events
(200 per batch or 2 s flush interval), and stages events via multi-row INSERT
or COPY inside a transaction. It preserves event UUIDs, inserts exact replays
once, and rejects conflicting contents for the same UUID without overwriting
history. Transient PG failures hold the batch unacknowledged with cancellable
1–30 s backoff. Invalid payloads, PG data exceptions and conflicting IDs are
isolated, copied to the DLQ with mandatory routing and publisher confirmation,
then individually acknowledged. Failed quarantine publication leaves the
original unacknowledged. Unrelated valid events can still commit.

**Query path** — `GET /admin/audit` is proxied through the router with session
auth and `admin.audit.read` authz. Supports cursor-based pagination, filtering
by scope, actor, action, target, service, request ID, and time range.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 8083 | HTTPS | HTTP ingest + admin query API |
| 9083 | HTTP | Health / readiness + Prometheus metrics |
| 9084 | gRPC | `AuditIngestService.RecordEvent` / `RecordEvents` |

## Configuration

YAML with `${VAR}` environment variable expansion. Loaded via
`csar-core/configload` (supports `CONFIG_SOURCE=file` or `manifest`).

```yaml
service:
  name: csar-audit
  port: 8083             # HTTP listen port
  health_port: 9083      # health/metrics sidecar

tls:                      # HTTP + query TLS (omit for plain HTTP in dev)
  cert_file: ""
  key_file: ""
  client_ca_file: ""
  min_version: "1.3"

grpc:
  port: 9084
  tls: {}                 # same structure as top-level tls
  reflection: true        # enable gRPC reflection (disable in prod if needed)

database:
  dsn: "${AUDIT_DATABASE_URL}"    # required — Postgres connection string

rabbitmq:
  url: "${RABBITMQ_URL}"         # required — AMQP(S) connection string
  reconnect_delay: 5s

ingest:
  receipt_timeout: 30s    # total receipt budget, including queue wait
  buffer_size: 10000      # in-memory channel depth
  publisher_workers: 4    # goroutines draining buffer to RabbitMQ

consumer:
  queue:
    name: audit.events
    durable: true
    prefetch: 200
  dlq:
    name: audit.events.dlq
    durable: true
  batch_size: 200         # max events per PG write
  flush_interval: 2s      # max time before flushing a partial batch
  max_redeliveries: 3     # deprecated; PG failures never imply poison

http:
  allowed_client_cn: ""   # mTLS client CN enforcement (empty = allow all)

tracing:
  endpoint: "${OTEL_EXPORTER_OTLP_ENDPOINT}"
  sample_ratio: 1.0
```

## Dependencies

| Dependency | Required | Notes |
|------------|----------|-------|
| PostgreSQL | Yes | Managed PG (Yandex Cloud) or local. Auto-migrates on startup. |
| RabbitMQ | Yes | Durable queues with publisher confirms. |
| csar-core | Yes | Shared primitives (config, HTTP, TLS, health, audit types). |
| csar-proto | Yes | Protobuf definitions for `AuditIngestService`. |

## Development

```bash
# Build
make build            # → bin/csar-audit

# Run (needs Postgres + RabbitMQ)
export AUDIT_DATABASE_URL="postgres://user:pass@localhost:5432/csar_audit?sslmode=disable"
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
make run

# Test
make test

# Lint
make lint
```

The root `go.work` selects the sibling `csar-core` and `csar-proto` modules
for local development. Standalone builds need released dependency versions
containing the event-identity contract; release protobuf bindings, then core,
then update audit dependency pins before building a production image.

### Replay integration tests

Set `AUDIT_TEST_DATABASE_URL` to an isolated PostgreSQL database named
`csar_audit_test` on `localhost` or `127.0.0.1`, then run `go test ./... -count=1`.
The store tests create and remove their own temporary schemas. They cover
small/bulk writes, repeated IDs within a batch, concurrent replay, JSONB
semantic equality and atomic rollback of conflicting payloads. Without this
explicit test URL the database integration tests are skipped.

### Producer identity and rollout

Call `audit.PrepareEvent` once and retain its returned event when manually
retrying a logical emission. Both ID and timestamp must remain unchanged.
The async client prepares its own event copy before sending; separately
recording the same ID-less input represents a new emission. Ingest also assigns
an ID and timestamp to legacy input before RabbitMQ publication. Queued events
own their JSON payload memory, so subsequent caller mutation cannot change them.

The gRPC contract adds optional UUID field `id` at field number 14. Upgrade all
audit consumers/ingest instances before producers: older consumers ignore IDs
and cannot provide the new deduplication behavior. Existing queued messages
without IDs remain readable but cannot reliably deduplicate on redelivery.

### Confirmed acceptance and quarantine

HTTP 202 and gRPC success require a routed, persistent broker-confirmed event,
rather than an in-memory enqueue. A batch has one receipt deadline (default
30 s). Saturation, shutdown, cancellation and missing receipts return failure;
a prefix may already be confirmed. Replay the full failed batch with the original IDs, timestamps and payloads. Legacy ID-less submissions cannot provide this replay guarantee.

The AMQP driver ignores PublishWithContext cancellation. A receipt deadline
therefore aborts the captured broker connection to bound channel RPCs and
socket writes. Other in-flight receipts on that connection may become uncertain;
the connection manager reconnects, and retained IDs make replay safe.

The checked-in audit-only RabbitMQ policy sets delivery-limit to -1 for both
audit queues, overriding the old five-delivery policy. This requires a broker
that accepts that policy: verified on deployed-version RabbitMQ 4.3.6. It is
rejected by 3.13; the three infra-node image pins now match 4.3.6. Do not
redeclare/delete existing queues. Check actual vhost, operator policies,
replica membership, disk headroom and queue/DLQ monitoring before activation.
Audit has no automatic DLQ consumer: inspect and explicitly replay quarantined
events after correcting their cause, retaining IDs.

The misleading historical metric name audit_events_dropped_total remains for
dashboard compatibility; it now counts failed/uncertain publication attempts,
not proof of lost events. audit_events_written_total counts persisted
deliveries acknowledged, including deduplicated replays.

This implementation requires compatible released protobuf/core dependencies
and upgraded audit receiver images before producer outboxes can be enabled.
Verify the built images and effective broker policy before rollout. Verify
TEMP privileges, transaction affinity and staging throughput through the
selected pooler. Authz and selected authn mutations now have optional producer outboxes in source.
Verified S3 archive jobs are also implemented, disabled by default. Partition
migration, cold-query access and retirement remain subsequent work.

### Isolated failure tests

Set AUDIT_TEST_DATABASE_URL to a localhost csar_audit_test PostgreSQL fixture
and AUDIT_TEST_AMQP_URL to the localhost audit-local user/rabbitmq vhost fixture.
Broker tests require empty idle audit queues, and never accept a remote host.
They test mandatory routing and 30 redeliveries for main and DLQ queues.
AUDIT_TEST_RESTART_STAGE=before/after brackets an externally controlled restart
of the isolated broker; AUDIT_TEST_BLOCKED_BROKER=true tests a receipt deadline
while that isolated broker is under an externally controlled memory alarm.
Leave these optional flags unset for normal tests. Never point these tests at
production or existing application queues.

## Database schema

Migrations run automatically on startup. The table is `audit_events`:

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | PK, producer ID preserved; assigned before queueing for legacy input |
| `service` | TEXT | Source service name |
| `actor` | TEXT | Subject who performed the action |
| `action` | TEXT | e.g. `campaign.create`, `role.assign` |
| `target_type` | TEXT | e.g. `campaign`, `user` |
| `target_id` | TEXT | Entity identifier |
| `scope_type` | TEXT | e.g. `tenant`, `platform` |
| `scope_id` | TEXT | Scope identifier |
| `before_state` | JSONB | Snapshot before mutation (nullable) |
| `after_state` | JSONB | Snapshot after mutation (nullable) |
| `metadata` | JSONB | Arbitrary context (nullable) |
| `request_id` | TEXT | Correlation ID from gateway |
| `client_ip` | TEXT | Originating IP |
| `created_at` | TIMESTAMPTZ | Event timestamp |

### Indexes

| Index | Columns | Use case |
|-------|---------|----------|
| `idx_audit_scope` | `(scope_type, scope_id, created_at DESC)` | Tenant-scoped event listing |
| `idx_audit_actor` | `(actor, created_at DESC)` | "What did user X do?" |
| `idx_audit_action` | `(action, created_at DESC)` | Filter by action type |
| `idx_audit_service` | `(service, created_at DESC)` | Filter by originating service |
| `idx_audit_request_id` | `(request_id) WHERE request_id != ''` | Request correlation lookup |
| `idx_audit_target` | `(target_type, target_id, created_at DESC)` | "What happened to entity X?" |
| `idx_audit_created_at` | `(created_at DESC, id DESC)` | Unfiltered pagination / time range |

## Deployment

The service runs as a Docker container alongside the rest of the CSAR stack
(router, authn, authz, etc.). A co-located RabbitMQ container provides the
message broker — no external AMQP service is needed.

Config is delivered via the S3 manifest protocol (`CONFIG_SOURCE=manifest`,
`CONFIG_MANIFEST_SERVICE=csar-audit`), published to Object Storage by CI
alongside all other service configs. The service picks up config changes on
its refresh interval (default 60 s).

### Router integration

The csar router proxies two surfaces to this service:

| Route | Method | Auth | Purpose |
|-------|--------|------|---------|
| `/admin/audit` | GET | Session + `admin.audit.read` authz | Admin query API |
| `/svc/audit/ingest` | POST | STS JWT (`csar-audit-svc` audience) | Service-to-service event ingest |

Both use the `audit-mtls` backend TLS policy for router-to-audit mTLS.

### TLS

Inter-service mTLS certs (`audit-server.pem`, `audit-client.pem`) are generated
by the shared TLS bootstrap script. The prod config expects them at
`/etc/csar/tls/audit-server.pem` and `/etc/csar/tls/audit-server-key.pem`.

## Health checks

- `GET /readiness` (port 9083) — checks Postgres, RabbitMQ connectivity, buffer saturation
- `GET /metrics` (port 9083) — Prometheus metrics (ingest, buffer, consumer, persistence)


## Verified logical archive (disabled by default)

`archive.enabled` defaults to false. Enabling is startup-only and needs an explicit
`environment`, `bucket`, `endpoint`, `region`, optional `prefix`, and `auth` using the
existing Yandex/static secret schema. No provider or bucket is implicitly chosen.
The destination must support version receipts, version-pinned GET and conditional
PUT (`If-None-Match: *`). Archive identities cannot delete or expire objects.
Object versioning is not an immutability or independent-copy guarantee by itself.

Migration 004 adds nullable `received_at` without backfilling old history, then
sets its default for future inserts. Exact replay keeps the first receipt. Old
NULL values are explicitly unknown in archives; they are never fabricated.

`internal/archive` produces gzip NDJSON chunks capped at 32MiB uncompressed,
33MiB compressed and 200 planned rows. Chunk descriptors contain schema version,
row count, SHA256 for both byte streams, event-ID coverage and timestamp bounds.
`Restore` verifies the entire pinned manifest/chunk before returning any rows.
It does not import a database, expose a public archive endpoint or authorize deletion.

A PostgreSQL planning transaction records fixed event membership and a stable job
UUID. One 180s lease coordinates replicas; SQL planning is bounded to 15s and the
whole job to 150s. Events with known receipts become eligible after one hour;
historical NULL receipts are eligible for backfill. Membership selection catches
old-timestamp events committed after earlier jobs. Jobs persist `planned` until
both objects are verified, then atomically become `catalogued`. Export/verification
are IO phases, never trusted catalog progress on their own. A stale worker cannot
finish another lease. Failed/lost receipts replay the same membership/key; conditional
PUT reuses existing versions and rechecks bytes. Conflicting existing bytes stop progress.
The worker waits at least one second between completed batches and 30s while idle/failing.

`audit_archive_pending_events`, `audit_archive_oldest_age_seconds` and
`audit_archive_scrape_success` expose lag, including planned-but-unverified rows.
Scrape SQL is capped at5s. Existing history uses occurred-time as the lag fallback,
so initial backfill deliberately raises lag alerts. Measure planner, membership
index growth and lag-query cost on a realistic clone before activation; the bounded
queries are not a production capacity proof. Keep current writes and enough free
space throughout backfill. Archive metadata/membership consumes additional PG space.

All hot rows remain. Membership foreign keys currently prevent deletion of planned
or archived rows. This is not the final partition-retirement schema. A complete hourly
job is not a sealed-partition proof. Automatic manifest discovery/catalog rebuild,
cold-query authorization, partitioning,
independent-copy evidence and retention/expiry remain activation/next-stage work.

Optional local tests use only AUDIT_TEST_DATABASE_URL on localhost/csar_audit_test.
They cover failed manifests, job replay, stale workers, late commits and preserved
receipt times. The fake S3 protocol tests in core cover both static and IAM modes,
version mismatch and conditional-write retries. A separate two-event live Yandex
preflight subsequently passed; it is not a production-history or capacity proof.
Configs and Prometheus rule source remain disabled/unpublished.

## Archive operator and recovery drill

Build the local operator with the matching core workspace/released dependency:

```bash
go build -o ./bin/csar-audit-archive ./cmd/csar-audit-archive
```

Use an archive-only protected config based on `ops/archive/reader.example.yaml`.
The tool deliberately rejects runtime configs containing database/broker settings.
It uses the dedicated reader, accepts an explicit version-pinned receipt and
prints only metadata/checksums/counts, not event payloads or credentials:

```bash
./bin/csar-audit-archive verify --config /protected/reader.yaml \
  --receipt-file /protected/receipt.json

# AUDIT_RESTORE_DATABASE_URL comes from the operator's protected environment.
# Only localhost/csar_audit_restore with sslmode=disable and no fallback is accepted.
./bin/csar-audit-archive restore-local --config /protected/reader.yaml \
  --receipt-file /protected/receipt.json
```

Verification completes before connecting to the isolated target. Its existing
audit migrations and transaction-local staging preserve original IDs, occurred
timestamps and receipt timestamps, including historical NULL. Exact repeated
imports succeed; conflicting payloads or receipts roll back the entire chunk
without overwriting stored events. Imported precision must match PG microseconds.
Local imports cap their statements at15s and the operation at150s. This is not a
production recovery command, a complete multi-manifest import, or a physical PITR.

`probe` writes exactly two synthetic records using `environment: preflight` and
creates a new0600 receipt file exclusively; `probe-replay` requires that receipt
and verifies conditional re-export leaves object versions unchanged. Probe files
and objects are retained on failure; no delete operation exists. A failed probe
may leave an empty receipt and uploaded objects; do not reuse its receipt path
or mistake it for a completed proof. Production history uses `environment: prod`.

The selected bucket is `aurumskynet-audit-prod`: private, versioned, default
KMS-encrypted using a separate deletion-protected key,64GiB bootstrap cap, no
lifecycle expiry. The writer has upload/pinned-verification access; the recovery
reader has read/version-list access. `ops/archive/bucket-policy.template.json`
denies insecure requests, unconditional PUT and history deletion. Substitute only
the exact reviewed bucket/account IDs; it is not an apply script for other buckets.
Conditional writes follow the [provider policy contract](https://yandex.cloud/en/docs/storage/concepts/policy).
Real credentials remain outside the repository and have not been distributed to
production. The independent copy, clone capacity, pooler and compatible-image
rollout checks remain open. Keep hot history throughout those checks.

The container image includes `/usr/local/bin/csar-audit-archive`. Use
`--entrypoint csar-audit-archive` for an operator invocation; the default entrypoint
continues to start the audit server. Restore remains guarded to the explicit local
fixture database. Mount only the protected reader configuration and receipt needed
for the operation.

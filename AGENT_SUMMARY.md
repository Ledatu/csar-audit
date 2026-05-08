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

## Audit Hotspots
- Trust is deployment-sensitive because `http.allowed_client_cn` is empty in prod config and the service relies on mTLS plus router placement.
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

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
(200 per batch or 2 s flush interval), and writes to PostgreSQL via multi-row
INSERT or COPY. Failed batches are NACKed so RabbitMQ retains them. Poison
messages (exceeding max redeliveries) are routed to a dead-letter queue.

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
  max_redeliveries: 3     # poison threshold → DLQ

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

Local `replace` directives in `go.mod` point to sibling directories
(`../csar-core`, `../csar-proto`), so you need the full workspace checkout.

## Database schema

Migrations run automatically on startup. The table is `audit_events`:

| Column | Type | Notes |
|--------|------|-------|
| `id` | UUID | PK, auto-generated |
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

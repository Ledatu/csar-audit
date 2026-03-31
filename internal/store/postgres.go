package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ledatu/csar-core/audit"
	"github.com/ledatu/csar-core/pgutil"
)

// BatchInserter is the write interface used by the consumer.
type BatchInserter interface {
	BatchInsert(ctx context.Context, events []audit.Event) error
}

// Lister is the read interface used by the query handler.
type Lister interface {
	List(ctx context.Context, filter *audit.ListFilter) (*audit.ListResult, error)
}

const copyFromThreshold = 50

var migrations = []pgutil.Migration{
	{
		Name: "001_audit_events",
		Up: `
CREATE TABLE IF NOT EXISTS audit_events (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor        TEXT NOT NULL,
    action       TEXT NOT NULL,
    target_type  TEXT NOT NULL,
    target_id    TEXT NOT NULL,
    scope_type   TEXT NOT NULL,
    scope_id     TEXT NOT NULL DEFAULT '',
    before_state JSONB,
    after_state  JSONB,
    metadata     JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_scope
    ON audit_events (scope_type, scope_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_actor
    ON audit_events (actor, created_at DESC);
`,
	},
	{
		Name: "002_audit_events_v2",
		Up: `
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS service TEXT NOT NULL DEFAULT '';
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT '';
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS client_ip TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS idx_audit_action
    ON audit_events (action, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_request_id
    ON audit_events (request_id) WHERE request_id != '';
CREATE INDEX IF NOT EXISTS idx_audit_service
    ON audit_events (service, created_at DESC);
`,
	},
	{
		Name: "003_audit_target_and_pagination",
		Up: `
CREATE INDEX IF NOT EXISTS idx_audit_target
    ON audit_events (target_type, target_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_created_at
    ON audit_events (created_at DESC, id DESC);
`,
	},
}

var copyFromColumns = []string{
	"service", "actor", "action", "target_type", "target_id",
	"scope_type", "scope_id", "before_state", "after_state", "metadata",
	"request_id", "client_ip", "created_at",
}

// Postgres persists audit events for csar-audit (batch writes + list).
type Postgres struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPostgres constructs a store backed by pool.
func NewPostgres(pool *pgxpool.Pool, logger *slog.Logger) *Postgres {
	if logger == nil {
		logger = slog.Default()
	}
	return &Postgres{pool: pool, logger: logger}
}

// Migrate applies audit schema migrations for this service.
func (s *Postgres) Migrate(ctx context.Context) error {
	return pgutil.RunMigrations(ctx, s.pool, "audit_service_migrations", migrations, s.logger)
}

// BatchInsert writes all events in a single database round-trip.
func (s *Postgres) BatchInsert(ctx context.Context, events []audit.Event) error {
	if len(events) == 0 {
		return nil
	}
	if len(events) > copyFromThreshold {
		return s.batchInsertCopyFrom(ctx, events)
	}
	return s.batchInsertMultiRow(ctx, events)
}

func (s *Postgres) batchInsertMultiRow(ctx context.Context, events []audit.Event) error {
	const colsPerRow = 13
	var sb strings.Builder
	sb.WriteString(`INSERT INTO audit_events (service, actor, action, target_type, target_id, scope_type, scope_id, before_state, after_state, metadata, request_id, client_ip, created_at) VALUES `)
	args := make([]any, 0, len(events)*colsPerRow)
	idx := 1
	for i := range events {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for c := 0; c < colsPerRow; c++ {
			if c > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "$%d", idx)
			idx++
		}
		sb.WriteByte(')')
		e := &events[i]
		created := e.CreatedAt
		if created.IsZero() {
			created = time.Now().UTC()
		}
		args = append(args,
			e.Service, e.Actor, e.Action, e.TargetType, e.TargetID,
			e.ScopeType, e.ScopeID,
			nullableJSON(e.BeforeState), nullableJSON(e.AfterState), nullableJSON(e.Metadata),
			e.RequestID, e.ClientIP,
			created,
		)
	}
	_, err := s.pool.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("batch insert audit events: %w", err)
	}
	return nil
}

func (s *Postgres) batchInsertCopyFrom(ctx context.Context, events []audit.Event) error {
	_, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"audit_events"},
		copyFromColumns,
		pgx.CopyFromSlice(len(events), func(i int) ([]any, error) {
			e := &events[i]
			created := e.CreatedAt
			if created.IsZero() {
				created = time.Now().UTC()
			}
			return []any{
				e.Service, e.Actor, e.Action, e.TargetType, e.TargetID,
				e.ScopeType, e.ScopeID,
				nullableJSON(e.BeforeState), nullableJSON(e.AfterState), nullableJSON(e.Metadata),
				e.RequestID, e.ClientIP,
				created,
			}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("copy audit events: %w", err)
	}
	return nil
}

// List returns a page of audit events using the same cursor contract as audit.PostgresStore.
func (s *Postgres) List(ctx context.Context, filter *audit.ListFilter) (*audit.ListResult, error) {
	if filter == nil {
		filter = &audit.ListFilter{}
	}
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	query := `SELECT id, service, actor, action, target_type, target_id, scope_type, scope_id,
	                  before_state, after_state, metadata, request_id, client_ip, created_at
	           FROM audit_events WHERE 1=1`
	args := []any{}
	idx := 1

	if filter.Service != "" {
		query += fmt.Sprintf(" AND service = $%d", idx)
		args = append(args, filter.Service)
		idx++
	}
	if filter.ScopeType != "" {
		query += fmt.Sprintf(" AND scope_type = $%d", idx)
		args = append(args, filter.ScopeType)
		idx++
	}
	if filter.ScopeID != "" {
		query += fmt.Sprintf(" AND scope_id = $%d", idx)
		args = append(args, filter.ScopeID)
		idx++
	}
	if filter.Actor != "" {
		query += fmt.Sprintf(" AND actor = $%d", idx)
		args = append(args, filter.Actor)
		idx++
	}
	if filter.Action != "" {
		query += fmt.Sprintf(" AND action = $%d", idx)
		args = append(args, filter.Action)
		idx++
	}
	if filter.TargetType != "" {
		query += fmt.Sprintf(" AND target_type = $%d", idx)
		args = append(args, filter.TargetType)
		idx++
	}
	if filter.TargetID != "" {
		query += fmt.Sprintf(" AND target_id = $%d", idx)
		args = append(args, filter.TargetID)
		idx++
	}
	if filter.RequestID != "" {
		query += fmt.Sprintf(" AND request_id = $%d", idx)
		args = append(args, filter.RequestID)
		idx++
	}
	if filter.Since != nil {
		query += fmt.Sprintf(" AND created_at >= $%d", idx)
		args = append(args, *filter.Since)
		idx++
	}
	if filter.Until != nil {
		query += fmt.Sprintf(" AND created_at <= $%d", idx)
		args = append(args, *filter.Until)
		idx++
	}
	if filter.Cursor != "" {
		if cursorTime, cursorID, err := audit.DecodeListCursor(filter.Cursor); err == nil {
			query += fmt.Sprintf(" AND (created_at < $%d OR (created_at = $%d AND id < $%d))",
				idx, idx, idx+1)
			args = append(args, cursorTime, cursorID)
			idx += 2
		}
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", idx)
	args = append(args, limit+1)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing audit events: %w", err)
	}
	defer rows.Close()

	var events []audit.Event
	for rows.Next() {
		var e audit.Event
		if err := rows.Scan(&e.ID, &e.Service, &e.Actor, &e.Action, &e.TargetType, &e.TargetID,
			&e.ScopeType, &e.ScopeID, &e.BeforeState, &e.AfterState, &e.Metadata,
			&e.RequestID, &e.ClientIP, &e.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning audit event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := &audit.ListResult{}
	if len(events) > limit {
		events = events[:limit]
		last := events[len(events)-1]
		result.NextCursor = audit.EncodeListCursor(last.CreatedAt, last.ID)
	}
	result.Events = events
	return result, nil
}

func nullableJSON(data json.RawMessage) any {
	if len(data) == 0 {
		return nil
	}
	return data
}

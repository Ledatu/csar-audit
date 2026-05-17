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
	ListGroups(ctx context.Context, filter *audit.ListFilter) (*GroupResult, error)
}

const copyFromThreshold = 50

const auditCategoryExpr = `CASE
WHEN service = 'csar-router' AND action LIKE 'GET /admin/audit%' THEN 'sensitive_read'
WHEN service = 'csar-router' THEN 'router_access'
WHEN action ~ '(^|\.)((authn|authz|sts|role|permission|service_account|session|user|security|credentials)\.|.*\.(assign|revoke|rotate|disable|enable|lock|unlock|merge|link|unlink)$)' THEN 'security_change'
WHEN action ~ '\.(create|update|delete|cancel|confirm|redeem|bootstrap|change|archive|start|stop|purge|reset|resolve|escalate)$' THEN 'business_mutation'
WHEN action ~ '\.(read|list|view|export|download)$' THEN 'sensitive_read'
ELSE 'system'
END`

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

// Group summarizes similar audit events for investigation views without
// replacing the raw append-only event trail.
type Group struct {
	Category     string    `json:"category"`
	Service      string    `json:"service,omitempty"`
	Actor        string    `json:"actor"`
	Action       string    `json:"action"`
	TargetType   string    `json:"target_type"`
	TargetID     string    `json:"target_id"`
	ScopeType    string    `json:"scope_type"`
	ScopeID      string    `json:"scope_id"`
	Count        int64     `json:"count"`
	SuccessCount int64     `json:"success_count"`
	FailureCount int64     `json:"failure_count"`
	FirstSeen    time.Time `json:"first_seen"`
	LastSeen     time.Time `json:"last_seen"`
}

// GroupResult is returned by the grouped investigation endpoint.
type GroupResult struct {
	Groups []Group `json:"groups"`
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

type whereBuilder struct {
	clauses []string
	args    []any
}

func (b *whereBuilder) nextArg(value any) string {
	b.args = append(b.args, value)
	return fmt.Sprintf("$%d", len(b.args))
}

func (b *whereBuilder) addEqual(column string, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	b.clauses = append(b.clauses, fmt.Sprintf("%s = %s", column, b.nextArg(value)))
}

func (b *whereBuilder) addNotIn(column string, values []string) {
	cleaned := cleanFilterValues(values)
	if len(cleaned) == 0 {
		return
	}
	placeholders := make([]string, 0, len(cleaned))
	for _, value := range cleaned {
		placeholders = append(placeholders, b.nextArg(value))
	}
	b.clauses = append(b.clauses, fmt.Sprintf("%s NOT IN (%s)", column, strings.Join(placeholders, ", ")))
}

func (b *whereBuilder) addTimeRange(filter *audit.ListFilter) {
	if filter.Since != nil {
		b.clauses = append(b.clauses, fmt.Sprintf("created_at >= %s", b.nextArg(*filter.Since)))
	}
	if filter.Until != nil {
		b.clauses = append(b.clauses, fmt.Sprintf("created_at <= %s", b.nextArg(*filter.Until)))
	}
}

func (b *whereBuilder) addStatusRange(filter *audit.ListFilter) {
	if filter.StatusMin <= 0 && filter.StatusMax <= 0 {
		return
	}
	b.clauses = append(b.clauses, "(metadata->>'http_status') ~ '^[0-9]+$'")
	if filter.StatusMin > 0 {
		b.clauses = append(b.clauses, fmt.Sprintf("(metadata->>'http_status')::int >= %s", b.nextArg(filter.StatusMin)))
	}
	if filter.StatusMax > 0 {
		b.clauses = append(b.clauses, fmt.Sprintf("(metadata->>'http_status')::int <= %s", b.nextArg(filter.StatusMax)))
	}
}

func (b *whereBuilder) addCursor(cursor string) {
	if cursor == "" {
		return
	}
	cursorTime, cursorID, err := audit.DecodeListCursor(cursor)
	if err != nil {
		return
	}
	timeArg := b.nextArg(cursorTime)
	idArg := b.nextArg(cursorID)
	b.clauses = append(b.clauses, fmt.Sprintf("(created_at < %s OR (created_at = %s AND id < %s))", timeArg, timeArg, idArg))
}

func (b *whereBuilder) whereSQL() string {
	if len(b.clauses) == 0 {
		return " WHERE 1=1"
	}
	return " WHERE 1=1 AND " + strings.Join(b.clauses, " AND ")
}

func categoryColumnSQL() string {
	return "(" + auditCategoryExpr + ")"
}

func cleanFilterValues(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if _, ok := seen[part]; ok {
				continue
			}
			seen[part] = struct{}{}
			cleaned = append(cleaned, part)
		}
	}
	return cleaned
}

func limitOrDefault(limit int) int {
	if limit <= 0 || limit > 100 {
		return 50
	}
	return limit
}

func buildWhere(filter *audit.ListFilter, includeCursor bool) (string, []any) {
	if filter == nil {
		filter = &audit.ListFilter{}
	}
	categoryColumn := categoryColumnSQL()
	builder := whereBuilder{}
	builder.addEqual("service", filter.Service)
	builder.addEqual("scope_type", filter.ScopeType)
	builder.addEqual("scope_id", filter.ScopeID)
	builder.addEqual("actor", filter.Actor)
	builder.addEqual("action", filter.Action)
	builder.addEqual(categoryColumn, filter.Category)
	builder.addEqual("target_type", filter.TargetType)
	builder.addEqual("target_id", filter.TargetID)
	builder.addEqual("request_id", filter.RequestID)
	builder.addNotIn(categoryColumn, filter.ExcludeCategories)
	builder.addNotIn("service", filter.ExcludeServices)
	builder.addNotIn("action", filter.ExcludeActions)
	builder.addStatusRange(filter)
	builder.addTimeRange(filter)
	if includeCursor {
		builder.addCursor(filter.Cursor)
	}
	return builder.whereSQL(), builder.args
}

// List returns a page of audit events using the same cursor contract as audit.PostgresStore.
func (s *Postgres) List(ctx context.Context, filter *audit.ListFilter) (*audit.ListResult, error) {
	if filter == nil {
		filter = &audit.ListFilter{}
	}
	limit := limitOrDefault(filter.Limit)

	whereSQL, args := buildWhere(filter, true)
	query := `SELECT id, service, ` + auditCategoryExpr + ` AS category, actor, action, target_type, target_id, scope_type, scope_id,
	                  before_state, after_state, metadata, request_id, client_ip, created_at
	           FROM audit_events` + whereSQL
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args)+1)
	args = append(args, limit+1)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing audit events: %w", err)
	}
	defer rows.Close()

	var events []audit.Event
	for rows.Next() {
		var e audit.Event
		if err := rows.Scan(&e.ID, &e.Service, &e.Category, &e.Actor, &e.Action, &e.TargetType, &e.TargetID,
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

// ListGroups returns grouped audit rows for high-volume investigation views.
func (s *Postgres) ListGroups(ctx context.Context, filter *audit.ListFilter) (*GroupResult, error) {
	if filter == nil {
		filter = &audit.ListFilter{}
	}
	limit := limitOrDefault(filter.Limit)
	whereSQL, args := buildWhere(filter, false)
	query := `SELECT ` + auditCategoryExpr + ` AS category, service, actor, action, target_type, target_id,
	                  scope_type, scope_id, COUNT(*) AS count,
	                  COUNT(*) FILTER (WHERE (metadata->>'http_status') ~ '^[0-9]+$' AND (metadata->>'http_status')::int BETWEEN 200 AND 399) AS success_count,
	                  COUNT(*) FILTER (WHERE (metadata->>'http_status') ~ '^[0-9]+$' AND (metadata->>'http_status')::int >= 400) AS failure_count,
	                  MIN(created_at) AS first_seen, MAX(created_at) AS last_seen
	           FROM audit_events` + whereSQL + `
	           GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
	           ORDER BY MAX(created_at) DESC
	           LIMIT $` + fmt.Sprint(len(args)+1)
	args = append(args, limit)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("grouping audit events: %w", err)
	}
	defer rows.Close()

	var groups []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.Category, &g.Service, &g.Actor, &g.Action, &g.TargetType, &g.TargetID,
			&g.ScopeType, &g.ScopeID, &g.Count, &g.SuccessCount, &g.FailureCount, &g.FirstSeen, &g.LastSeen); err != nil {
			return nil, fmt.Errorf("scanning audit group: %w", err)
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &GroupResult{Groups: groups}, nil
}

func nullableJSON(data json.RawMessage) any {
	if len(data) == 0 {
		return nil
	}
	return data
}

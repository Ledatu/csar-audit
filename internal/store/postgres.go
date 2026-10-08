package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
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
	List(ctx context.Context, filter *ListFilter) (*ListResult, error)
	ListGroups(ctx context.Context, filter *ListFilter) (*GroupResult, error)
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
	{Name: "004_canonical_receipt", Up: `
-- Existing history has no reliable receipt time. Only future inserts get a default.
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS received_at timestamptz;
ALTER TABLE audit_events ALTER COLUMN received_at SET DEFAULT clock_timestamp();
`},
}

var copyFromColumns = []string{
	"id", "service", "actor", "action", "target_type", "target_id",
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

// Event is the csar-audit query response shape. It mirrors audit.Event and adds
// derived fields that are local to the query surface.
type Event struct {
	ID          string          `json:"id"`
	Service     string          `json:"service,omitempty"`
	Category    string          `json:"category,omitempty"`
	Actor       string          `json:"actor"`
	Action      string          `json:"action"`
	TargetType  string          `json:"target_type"`
	TargetID    string          `json:"target_id"`
	ScopeType   string          `json:"scope_type"`
	ScopeID     string          `json:"scope_id"`
	BeforeState json.RawMessage `json:"before_state,omitempty"`
	AfterState  json.RawMessage `json:"after_state,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	RequestID   string          `json:"request_id,omitempty"`
	ClientIP    string          `json:"client_ip,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
}

// ListFilter specifies query parameters for listing audit events.
type ListFilter struct {
	ScopeType         string
	ScopeID           string
	Service           string
	Actor             string
	Action            string
	Category          string
	TargetType        string
	TargetID          string
	RequestID         string
	ExcludeCategories []string
	ExcludeServices   []string
	ExcludeActions    []string
	StatusMin         int
	StatusMax         int
	Since             *time.Time
	Until             *time.Time
	Cursor            string
	Limit             int
}

// ListResult holds a page of audit events with an optional next cursor.
type ListResult struct {
	Events     []Event `json:"events"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

// Group summarizes similar audit events for investigation views without
// replacing the raw append-only event trail.
type Group struct {
	BucketStart  time.Time `json:"bucket_start"`
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

// EventConflictError means an ID was reused with different event contents.
// The entire batch is rolled back; existing events are never overwritten.
type EventConflictError struct {
	ID string
}

func (e *EventConflictError) Error() string {
	return fmt.Sprintf("audit event ID %s has conflicting contents", e.ID)
}

// BatchInsert atomically persists a batch and verifies duplicate IDs against
// existing contents. Exact replays succeed without inserting additional rows.
func (s *Postgres) BatchInsert(ctx context.Context, events []audit.Event) error {
	return s.batchInsert(ctx, events, nil, false)
}

// RestoreBatch preserves archived receipt times, including unknown historical
// NULL values. Only the guarded isolated restore command calls this path.
// Existing events must match both content and canonical receipt; no overwrite.
func (s *Postgres) RestoreBatch(ctx context.Context, events []audit.Event, receipts []*time.Time) error {
	if len(events) != len(receipts) {
		return fmt.Errorf("restore receipt count mismatch")
	}
	seen := make(map[string]struct{}, len(events))
	for i := range events {
		id, err := uuid.Parse(events[i].ID)
		if err != nil || id == uuid.Nil || id.String() != events[i].ID || events[i].CreatedAt.IsZero() {
			return fmt.Errorf("restore requires existing event identity and timestamp")
		}
		key := events[i].ID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("restore duplicate event ID")
		}
		seen[key] = struct{}{}
		if !events[i].CreatedAt.Equal(events[i].CreatedAt.Truncate(time.Microsecond)) || (receipts[i] != nil && (receipts[i].IsZero() || !receipts[i].Equal(receipts[i].Truncate(time.Microsecond)))) {
			return fmt.Errorf("restore timestamps must preserve PostgreSQL microsecond precision")
		}
	}
	return s.batchInsert(ctx, events, receipts, true)
}

func (s *Postgres) batchInsert(ctx context.Context, events []audit.Event, receipts []*time.Time, restoring bool) error {
	if len(events) == 0 {
		return nil
	}
	prepared := make([]audit.Event, len(events))
	for i := range events {
		event, err := audit.PrepareEvent(&events[i])
		if err != nil {
			return fmt.Errorf("prepare audit batch event: %w", err)
		}
		prepared[i] = *event
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin audit batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// No indexes/defaults: this table only holds the incoming values for the
	// current transaction, including repeated IDs within one delivery batch.
	if _, err := tx.Exec(ctx, `CREATE TEMP TABLE audit_event_batch
		(LIKE audit_events) ON COMMIT DROP`); err != nil {
		return fmt.Errorf("create audit batch staging: %w", err)
	}
	if len(events) > copyFromThreshold {
		err = s.batchInsertCopyFrom(ctx, tx, prepared)
	} else {
		err = s.batchInsertMultiRow(ctx, tx, prepared)
	}
	if err != nil {
		return err
	}
	columns := strings.Join(copyFromColumns, ", ")
	if restoring {
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE audit_restore_receipts(id uuid,received_at timestamptz) ON COMMIT DROP`); err != nil {
			return fmt.Errorf("create restore receipt staging: %w", err)
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"pg_temp", "audit_restore_receipts"}, []string{"id", "received_at"}, pgx.CopyFromSlice(len(prepared), func(i int) ([]any, error) {
			return []any{prepared[i].ID, receipts[i]}, nil
		})); err != nil {
			return fmt.Errorf("stage restore receipts: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE pg_temp.audit_event_batch e SET received_at=r.received_at FROM pg_temp.audit_restore_receipts r WHERE e.id=r.id`); err != nil {
			return fmt.Errorf("restore receipt timestamps: %w", err)
		}
		columns += ", received_at"
	}
	// Stable lock order prevents concurrent overlapping batches from acquiring
	// unique-index locks in opposite order.
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events (`+columns+`)
		SELECT `+columns+` FROM pg_temp.audit_event_batch ORDER BY id
		ON CONFLICT (id) DO NOTHING`); err != nil {
		return fmt.Errorf("insert audit batch: %w", err)
	}
	// A separate READ COMMITTED statement also sees a concurrently committed
	// row that caused ON CONFLICT to wait. JSONB comparison ignores key order.
	var conflictingID string
	receiptConflict := ""
	if restoring {
		receiptConflict = " OR incoming.received_at IS DISTINCT FROM stored.received_at"
	}
	err = tx.QueryRow(ctx, `SELECT incoming.id::text
		FROM pg_temp.audit_event_batch incoming JOIN audit_events stored USING (id)
		WHERE ROW(incoming.service, incoming.actor, incoming.action,
		 incoming.target_type, incoming.target_id, incoming.scope_type, incoming.scope_id,
		 incoming.before_state, incoming.after_state, incoming.metadata,
		 incoming.request_id, incoming.client_ip, incoming.created_at)
		IS DISTINCT FROM ROW(stored.service, stored.actor, stored.action,
		 stored.target_type, stored.target_id, stored.scope_type, stored.scope_id,
		 stored.before_state, stored.after_state, stored.metadata,
		 stored.request_id, stored.client_ip, stored.created_at)`+receiptConflict+`
		LIMIT 1`).Scan(&conflictingID)
	if err == nil {
		return &EventConflictError{ID: conflictingID}
	}
	if err != pgx.ErrNoRows {
		return fmt.Errorf("verify audit batch replays: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit audit batch: %w", err)
	}
	return nil
}

func (s *Postgres) batchInsertMultiRow(ctx context.Context, tx pgx.Tx, events []audit.Event) error {
	const colsPerRow = 14
	var sb strings.Builder
	sb.WriteString(`INSERT INTO pg_temp.audit_event_batch (` + strings.Join(copyFromColumns, ", ") + `) VALUES `)
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
		args = append(args,
			e.ID, e.Service, e.Actor, e.Action, e.TargetType, e.TargetID,
			e.ScopeType, e.ScopeID,
			nullableJSON(e.BeforeState), nullableJSON(e.AfterState), nullableJSON(e.Metadata),
			e.RequestID, e.ClientIP,
			e.CreatedAt,
		)
	}
	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("batch insert audit events: %w", err)
	}
	return nil
}

func (s *Postgres) batchInsertCopyFrom(ctx context.Context, tx pgx.Tx, events []audit.Event) error {
	_, err := tx.CopyFrom(ctx,
		pgx.Identifier{"pg_temp", "audit_event_batch"},
		copyFromColumns,
		pgx.CopyFromSlice(len(events), func(i int) ([]any, error) {
			e := &events[i]
			return []any{
				e.ID, e.Service, e.Actor, e.Action, e.TargetType, e.TargetID,
				e.ScopeType, e.ScopeID,
				nullableJSON(e.BeforeState), nullableJSON(e.AfterState), nullableJSON(e.Metadata),
				e.RequestID, e.ClientIP,
				e.CreatedAt,
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

func (b *whereBuilder) addTimeRange(filter *ListFilter) {
	if filter.Since != nil {
		b.clauses = append(b.clauses, fmt.Sprintf("created_at >= %s", b.nextArg(*filter.Since)))
	}
	if filter.Until != nil {
		b.clauses = append(b.clauses, fmt.Sprintf("created_at <= %s", b.nextArg(*filter.Until)))
	}
}

func (b *whereBuilder) addStatusRange(filter *ListFilter) {
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

func buildWhere(filter *ListFilter, includeCursor bool) (string, []any) {
	if filter == nil {
		filter = &ListFilter{}
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
func (s *Postgres) List(ctx context.Context, filter *ListFilter) (*ListResult, error) {
	if filter == nil {
		filter = &ListFilter{}
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

	var events []Event
	for rows.Next() {
		var e Event
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

	result := &ListResult{}
	if len(events) > limit {
		events = events[:limit]
		last := events[len(events)-1]
		result.NextCursor = audit.EncodeListCursor(last.CreatedAt, last.ID)
	}
	result.Events = events
	return result, nil
}

// ListGroups returns grouped audit rows for high-volume investigation views.
func (s *Postgres) ListGroups(ctx context.Context, filter *ListFilter) (*GroupResult, error) {
	if filter == nil {
		filter = &ListFilter{}
	}
	limit := limitOrDefault(filter.Limit)
	whereSQL, args := buildWhere(filter, false)
	query := `SELECT date_trunc('hour', created_at) AS bucket_start,
	                  ` + auditCategoryExpr + ` AS category, service, actor, action, target_type, target_id,
	                  scope_type, scope_id, COUNT(*) AS count,
	                  COUNT(*) FILTER (WHERE (metadata->>'http_status') ~ '^[0-9]+$' AND (metadata->>'http_status')::int BETWEEN 200 AND 399) AS success_count,
	                  COUNT(*) FILTER (WHERE (metadata->>'http_status') ~ '^[0-9]+$' AND (metadata->>'http_status')::int >= 400) AS failure_count,
	                  MIN(created_at) AS first_seen, MAX(created_at) AS last_seen
	           FROM audit_events` + whereSQL + `
	           GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
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
		if err := rows.Scan(&g.BucketStart, &g.Category, &g.Service, &g.Actor, &g.Action, &g.TargetType, &g.TargetID,
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

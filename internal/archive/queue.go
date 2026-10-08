package archive

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/ledatu/csar-core/pgutil"
)

const bootstrapBatch = 1000

// Trigger functions run with their owner's rights so INSERT-only legacy writers
// can keep ingesting. The migration substitutes a quoted, trusted schema and
// pins pg_temp last; writer-controlled search_path never resolves these tables.
const queueSchema = `
SET LOCAL lock_timeout='3s';
LOCK TABLE audit_events IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE audit_archive_pending (
 event_id uuid PRIMARY KEY REFERENCES audit_events(id) ON DELETE RESTRICT,
 eligible_at timestamptz NOT NULL, oldest_at timestamptz NOT NULL,
 job_id uuid REFERENCES audit_archive_jobs(id) ON DELETE RESTRICT
) WITH (autovacuum_vacuum_scale_factor=0.02,autovacuum_vacuum_threshold=1000,
 autovacuum_analyze_scale_factor=0.02,autovacuum_analyze_threshold=1000);
CREATE INDEX audit_archive_pending_ready ON audit_archive_pending(eligible_at,event_id) WHERE job_id IS NULL;
CREATE INDEX audit_archive_pending_oldest ON audit_archive_pending(oldest_at,event_id);
CREATE INDEX audit_archive_pending_job ON audit_archive_pending(job_id) WHERE job_id IS NOT NULL;
CREATE TABLE audit_archive_pending_counts (
 bucket integer PRIMARY KEY CHECK(bucket>=0 AND bucket<32), events bigint NOT NULL CHECK(events>=0)
);
INSERT INTO audit_archive_pending_counts SELECT i,0 FROM generate_series(0,31) g(i);
CREATE TABLE audit_archive_queue_state (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 upper_id uuid, cursor_id uuid, complete boolean NOT NULL
);

CREATE FUNCTION audit_archive_capture_events() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,__SCHEMA__,pg_temp AS $$
BEGIN
 INSERT INTO audit_archive_pending(event_id,eligible_at,oldest_at)
 SELECT id,COALESCE(received_at+interval '1 hour','-infinity'::timestamptz),COALESCE(received_at,created_at)
 FROM inserted_events ORDER BY id ON CONFLICT DO NOTHING;
 RETURN NULL;
END $$;
CREATE TRIGGER audit_archive_capture_events AFTER INSERT ON audit_events
 REFERENCING NEW TABLE AS inserted_events FOR EACH STATEMENT EXECUTE FUNCTION audit_archive_capture_events();

CREATE FUNCTION audit_archive_refresh_receipt() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,__SCHEMA__,pg_temp AS $$
BEGIN
 UPDATE audit_archive_pending SET
 eligible_at=COALESCE(NEW.received_at+interval '1 hour','-infinity'::timestamptz),
 oldest_at=COALESCE(NEW.received_at,NEW.created_at) WHERE event_id=NEW.id;
 RETURN NULL;
END $$;
CREATE TRIGGER audit_archive_refresh_receipt AFTER UPDATE OF received_at,created_at ON audit_events
 FOR EACH ROW EXECUTE FUNCTION audit_archive_refresh_receipt();

CREATE FUNCTION audit_archive_count_insert() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,__SCHEMA__,pg_temp AS $$
DECLARE r record;
BEGIN
 -- Hash the whole UUID: time-prefixed UUIDs must not concentrate in one bucket.
 -- Sorted locks avoid opposing acquisition orders within a batch statement.
 FOR r IN SELECT get_byte(decode(md5(event_id::text),'hex'),0)%32 AS bucket,count(*) AS n
 FROM inserted_pending GROUP BY 1 ORDER BY 1 LOOP
  UPDATE audit_archive_pending_counts SET events=events+r.n WHERE bucket=r.bucket;
 END LOOP;
 RETURN NULL;
END $$;
CREATE TRIGGER audit_archive_count_insert AFTER INSERT ON audit_archive_pending
 REFERENCING NEW TABLE AS inserted_pending FOR EACH STATEMENT EXECUTE FUNCTION audit_archive_count_insert();
CREATE FUNCTION audit_archive_count_delete() RETURNS trigger
 LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,__SCHEMA__,pg_temp AS $$
DECLARE r record;
BEGIN
 FOR r IN SELECT get_byte(decode(md5(event_id::text),'hex'),0)%32 AS bucket,count(*) AS n
 FROM deleted_pending GROUP BY 1 ORDER BY 1 LOOP
  UPDATE audit_archive_pending_counts SET events=events-r.n WHERE bucket=r.bucket;
 END LOOP;
 RETURN NULL;
END $$;
CREATE TRIGGER audit_archive_count_delete AFTER DELETE ON audit_archive_pending
 REFERENCING OLD TABLE AS deleted_pending FOR EACH STATEMENT EXECUTE FUNCTION audit_archive_count_delete();
REVOKE ALL ON FUNCTION audit_archive_capture_events(),audit_archive_refresh_receipt(),
 audit_archive_count_insert(),audit_archive_count_delete() FROM PUBLIC;

INSERT INTO audit_archive_queue_state(singleton,upper_id,complete)
 SELECT true,id,id IS NULL FROM (SELECT (SELECT id FROM audit_events ORDER BY id DESC LIMIT 1) AS id) bounds;
`

func migrateQueue(ctx context.Context, tx pgx.Tx) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname='audit_archive_queue_state')`).Scan(&exists); err != nil || exists {
		return err
	}
	var schema string
	if err := tx.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, strings.ReplaceAll(queueSchema, "__SCHEMA__", pgx.Identifier{schema}.Sanitize()))
	return err
}

// Backfill examines one primary-key page BEFORE consulting archive membership.
// The insert trigger covers every concurrent/late event, including UUIDs below
// the cursor. The cursor only reconciles pre-trigger history and is finite.
func (j *Jobs) Backfill(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, archiveDBTimeout)
	defer cancel()
	var progress bool
	err := pgutil.WithTx(ctx, j.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(839106410825)`); err != nil {
			return err
		}
		var cursor, upper *string
		var complete bool
		if err := tx.QueryRow(ctx, `SELECT cursor_id::text,upper_id::text,complete FROM audit_archive_queue_state WHERE singleton FOR UPDATE`).Scan(&cursor, &upper, &complete); err != nil || complete {
			return err
		}
		query := `SELECT id::text FROM audit_events WHERE id<=$1::uuid ORDER BY id LIMIT $2`
		args := []any{upper, bootstrapBatch}
		if cursor != nil {
			query = `SELECT id::text FROM audit_events WHERE id>$1::uuid AND id<=$2::uuid ORDER BY id LIMIT $3`
			args = []any{cursor, upper, bootstrapBatch}
		}
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		progress = len(ids) > 0
		if progress {
			if _, err := tx.Exec(ctx, `INSERT INTO audit_archive_pending(event_id,eligible_at,oldest_at,job_id)
 SELECT e.id,COALESCE(e.received_at+interval '1 hour','-infinity'::timestamptz),COALESCE(e.received_at,e.created_at),m.job_id
 FROM unnest($1::uuid[]) selected(id) JOIN audit_events e ON e.id=selected.id
 LEFT JOIN audit_archive_members m ON m.event_id=e.id LEFT JOIN audit_archive_jobs j ON j.id=m.job_id
 WHERE j.state IS DISTINCT FROM 'catalogued' ORDER BY e.id ON CONFLICT DO NOTHING`, ids); err != nil {
				return err
			}
			cursor = &ids[len(ids)-1]
		}
		_, err = tx.Exec(ctx, `UPDATE audit_archive_queue_state SET cursor_id=$1,complete=$2 WHERE singleton`, cursor, len(ids) < bootstrapBatch || (cursor != nil && upper != nil && *cursor == *upper))
		return err
	})
	return progress, err
}

const pendingMetricsQuery = `SELECT
 (SELECT CASE WHEN count(*)=32 THEN sum(events)::bigint END FROM audit_archive_pending_counts),
 COALESCE(GREATEST(extract(epoch FROM statement_timestamp()-(SELECT oldest_at FROM audit_archive_pending ORDER BY oldest_at,event_id LIMIT 1)),0),0)::float8,
 (SELECT complete FROM audit_archive_queue_state WHERE singleton)`

const candidateQuery = `WITH selected AS MATERIALIZED (
 SELECT event_id,eligible_at FROM audit_archive_pending WHERE job_id IS NULL
 AND eligible_at<=statement_timestamp() ORDER BY eligible_at,event_id LIMIT 200
), candidates AS MATERIALIZED (
 SELECT e.id,s.eligible_at,6::bigint*octet_length(to_jsonb(e)::text)+1024 AS budget
 FROM selected s JOIN audit_events e ON e.id=s.event_id
), bounded AS (
 SELECT id,sum(budget) OVER(ORDER BY eligible_at,id) AS cumulative FROM candidates
) SELECT ` + recordProjection + ` FROM bounded JOIN audit_events e ON e.id=bounded.id
 WHERE cumulative<=$1 ORDER BY e.created_at,e.id`

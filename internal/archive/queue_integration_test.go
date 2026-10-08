package archive

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/audit"
	"github.com/prometheus/client_golang/prometheus"
)

// Reconstruct the already-published pre-queue schema inside the guarded fixture.
func legacyQueueFixture(t *testing.T) (*Jobs, *store.Postgres) {
	t.Helper()
	j, pg := jobsFixture(t)
	_, err := j.Pool.Exec(context.Background(), `
DROP TRIGGER audit_archive_capture_events ON audit_events;
DROP TRIGGER audit_archive_refresh_receipt ON audit_events;
DROP TABLE audit_archive_pending;
DROP TABLE audit_archive_pending_counts;
DROP TABLE audit_archive_queue_state;
DROP FUNCTION audit_archive_capture_events(),audit_archive_refresh_receipt(),audit_archive_count_insert(),audit_archive_count_delete();`)
	if err != nil {
		t.Fatal(err)
	}
	return j, pg
}

func pendingCount(t *testing.T, j *Jobs, want int64, complete bool) {
	t.Helper()
	var count, actual int64
	var age float64
	var done bool
	if err := j.Pool.QueryRow(context.Background(), pendingMetricsQuery).Scan(&count, &age, &done); err != nil {
		t.Fatal(err)
	}
	if err := j.Pool.QueryRow(context.Background(), `SELECT count(*) FROM audit_archive_pending`).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if count != want || actual != want || done != complete || age < 0 {
		t.Fatalf("pending counters=%d actual=%d bootstrap=%v; want %d/%v", count, actual, done, want, complete)
	}
}

func TestQueueMigrationWaitsForOldWriter(t *testing.T) {
	j, _ := legacyQueueFixture(t)
	ctx := context.Background()
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	id := "ffffffff-ffff-4fff-8fff-ffffffffffff"
	if _, err := tx.Exec(ctx, `INSERT INTO audit_events(id,actor,action,target_type,target_id,scope_type,received_at) VALUES($1,'test','test.update','test','1','platform',NULL)`, id); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- j.Migrate(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var waiting bool
		if err := j.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='audit_events'::regclass AND mode='ShareRowExclusiveLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("migration did not wait on pre-trigger writer")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var upper string
	if err := j.Pool.QueryRow(ctx, `SELECT upper_id::text FROM audit_archive_queue_state`).Scan(&upper); err != nil || upper != id {
		t.Fatal("pre-trigger commit missing from bootstrap ceiling", upper, err)
	}
	if progress, err := j.Backfill(ctx); err != nil || !progress {
		t.Fatal("old commit not reconciled", err)
	}
	pendingCount(t, j, 1, true)
	if err := j.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pendingCount(t, j, 1, true)
}

func TestQueueBoundedBootstrapRestartAndLowerUUID(t *testing.T) {
	j, pg := legacyQueueFixture(t)
	ctx := context.Background()
	if _, err := j.Pool.Exec(ctx, `INSERT INTO audit_events(id,actor,action,target_type,target_id,scope_type,received_at)
 SELECT ('10000000-0000-4000-8000-'||lpad(i::text,12,'0'))::uuid,'test','test.update','test','1','platform',NULL FROM generate_series(1,2005) g(i)`); err != nil {
		t.Fatal(err)
	}
	if err := j.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pendingCount(t, j, 0, false)
	if progress, err := j.Backfill(ctx); err != nil || !progress {
		t.Fatal(err)
	}
	pendingCount(t, j, bootstrapBatch, false)
	// A new UUID below the scanned cursor cannot be discovered by a high-water
	// scan. The trigger must capture it, including its historical NULL receipt.
	e := sample(t)[0].Event
	e.ID = "00000000-0000-4000-8000-000000000001"
	if err := pg.RestoreBatch(ctx, []audit.Event{e}, []*time.Time{nil}); err != nil {
		t.Fatal(err)
	}
	pendingCount(t, j, bootstrapBatch+1, false)
	restarted := &Jobs{Pool: j.Pool}
	if err := restarted.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if progress, err := restarted.Backfill(ctx); err != nil || !progress {
			t.Fatal(err)
		}
	}
	pendingCount(t, j, 2006, true)
	if progress, err := restarted.Backfill(ctx); err != nil || progress {
		t.Fatal("completed bootstrap repeated history", err)
	}
	if err := pg.RestoreBatch(ctx, []audit.Event{e}, []*time.Time{nil}); err != nil {
		t.Fatal(err)
	}
	pendingCount(t, j, 2006, true)
}

func TestLegacyJobRepairAndBootstrapCompleteRace(t *testing.T) {
	j, pg := legacyQueueFixture(t)
	ctx := context.Background()
	record := insertHistorical(t, j, pg)
	desc, err := describe([]Record{record})
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.NewString()
	if _, err := j.Pool.Exec(ctx, `INSERT INTO audit_archive_jobs(id,environment,rows,coverage_sha256) VALUES($1,'prod',1,$2);
`, id, desc.EventIDsSHA256); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Pool.Exec(ctx, `INSERT INTO audit_archive_members(event_id,job_id) VALUES($1,$2)`, record.Event.ID, id); err != nil {
		t.Fatal(err)
	}
	if err := j.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := j.Claim(ctx, "prod")
	if err != nil || job == nil || job.ID != id {
		t.Fatal("legacy planned job not repaired", err)
	}
	pendingCount(t, j, 1, false)
	records, err := j.Load(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := Export(ctx, &fakeObjects{}, job.Environment, job.ID, job.PlannedAt, records)
	if err != nil {
		t.Fatal(err)
	}
	// Hold the planning lock so both operations race once released. Either order
	// must leave no pending resurrection and exactly one verified membership.
	tx, err := j.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(839106410825)`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	go func() { _, err := j.Backfill(ctx); done <- err }()
	go func() { done <- j.Complete(ctx, job, receipt) }()
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	pendingCount(t, j, 0, true)
	if err := j.Complete(ctx, job, receipt); !errors.Is(err, ErrArchiveLeaseLost) {
		t.Fatal("old token changed queue", err)
	}
	pendingCount(t, j, 0, true)
}

func TestQueueCaptureRestrictedWriterAndRollback(t *testing.T) {
	j, _ := jobsFixture(t)
	ctx := context.Background()
	var schema string
	if err := j.Pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	role := pgx.Identifier{"audit_queue_writer_" + uuid.NewString()[:8]}.Sanitize()
	if _, err := j.Pool.Exec(ctx, `CREATE ROLE `+role); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := j.Pool.Exec(ctx, `DROP OWNED BY `+role+`; DROP ROLE `+role); err != nil {
			t.Error(err)
		}
	})
	table := pgx.Identifier{schema, "audit_events"}.Sanitize()
	if _, err := j.Pool.Exec(ctx, `GRANT USAGE ON SCHEMA `+pgx.Identifier{schema}.Sanitize()+` TO `+role+`; GRANT INSERT ON `+table+` TO `+role); err != nil {
		t.Fatal(err)
	}
	for _, commit := range []bool{false, true} {
		tx, err := j.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+role+`; SET LOCAL search_path=pg_catalog,pg_temp`); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO `+table+`(id,actor,action,target_type,target_id,scope_type,received_at) VALUES($1,'test','test.update','test','1','platform',NULL)`, uuid.NewString()); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal("INSERT-only writer failed trigger", err)
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	pendingCount(t, j, 1, true)
}

func TestQueueCountersConcurrentBatches(t *testing.T) {
	j, pg := jobsFixture(t)
	ctx := context.Background()
	done := make(chan error, 4)
	for writer := 0; writer < 4; writer++ {
		go func() {
			events := make([]audit.Event, 200)
			for i := range events {
				events[i] = audit.Event{ID: uuid.NewString(), Actor: "test", Action: "test.update", TargetType: "test", TargetID: fmt.Sprint(i), ScopeType: "platform", CreatedAt: time.Now().UTC()}
			}
			done <- pg.BatchInsert(ctx, events)
		}()
	}
	for writer := 0; writer < 4; writer++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	pendingCount(t, j, 800, true)
}

func TestQueueMetricsBootstrapAndMissingCounter(t *testing.T) {
	j, pg := legacyQueueFixture(t)
	ctx := context.Background()
	insertHistorical(t, j, pg)
	if err := j.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	registry := prometheus.NewRegistry()
	if err := j.RegisterMetrics(registry); err != nil {
		t.Fatal(err)
	}
	read := func() map[string]float64 {
		t.Helper()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		values := make(map[string]float64)
		for _, family := range families {
			values[family.GetName()] = family.Metric[0].GetGauge().GetValue()
		}
		return values
	}
	before := read()
	if before["audit_archive_bootstrap_complete"] != 0 || before["audit_archive_pending_events"] != 0 || before["audit_archive_scrape_success"] != 1 {
		t.Fatal("incomplete history discovery not explicit", before)
	}
	if _, err := j.Backfill(ctx); err != nil {
		t.Fatal(err)
	}
	after := read()
	if after["audit_archive_bootstrap_complete"] != 1 || after["audit_archive_pending_events"] != 1 || after["audit_archive_oldest_age_seconds"] < 3600 {
		t.Fatal("backfilled backlog metrics incorrect", after)
	}
	if _, err := j.Pool.Exec(ctx, `DELETE FROM audit_archive_pending_counts WHERE bucket=0`); err != nil {
		t.Fatal(err)
	}
	broken := read()
	if _, exists := broken["audit_archive_pending_events"]; exists || broken["audit_archive_scrape_success"] != 0 {
		t.Fatal("missing counter presented as healthy empty history", broken)
	}
}

func TestQueueCompletionMismatchRollsBackCatalogue(t *testing.T) {
	j, pg := jobsFixture(t)
	ctx := context.Background()
	insertHistorical(t, j, pg)
	job, err := j.Claim(ctx, "prod")
	if err != nil || job == nil {
		t.Fatal(err)
	}
	records, err := j.Load(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := Export(ctx, &fakeObjects{}, job.Environment, job.ID, job.PlannedAt, records)
	if err != nil {
		t.Fatal(err)
	}
	extra := insertHistorical(t, j, pg)
	if _, err := j.Pool.Exec(ctx, `UPDATE audit_archive_pending SET job_id=$1 WHERE event_id=$2`, job.ID, extra.Event.ID); err != nil {
		t.Fatal(err)
	}
	if err := j.Complete(ctx, job, receipt); err == nil {
		t.Fatal("mismatched pending membership accepted")
	}
	var planned bool
	if err := j.Pool.QueryRow(ctx, `SELECT state='planned' AND receipt IS NULL AND lease_token=$2 FROM audit_archive_jobs WHERE id=$1`, job.ID, job.Token).Scan(&planned); err != nil || !planned {
		t.Fatal("failed completion advanced catalogue", err)
	}
	pendingCount(t, j, 2, true)
}

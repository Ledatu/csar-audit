package archive

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/audit"
)

func jobsFixture(t *testing.T) (*Jobs, *store.Postgres) {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated localhost archive database not configured")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if (cfg.ConnConfig.Host != "localhost" && cfg.ConnConfig.Host != "127.0.0.1") || cfg.ConnConfig.Database != "csar_audit_test" || len(cfg.ConnConfig.Fallbacks) > 0 {
		t.Fatal("only isolated localhost csar_audit_test accepted")
	}
	ctx := context.Background()
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"archive_test_" + uuid.NewString()[:8]}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
		if err != nil {
			t.Error(err)
		}
	})
	pg := store.NewPostgres(pool, nil)
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	jobs := &Jobs{Pool: pool}
	if err := jobs.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return jobs, pg
}

func insertHistorical(t *testing.T, j *Jobs, pg *store.Postgres) Record {
	t.Helper()
	record := sample(t)[0]
	record.Event.CreatedAt = time.Now().UTC().Add(-12 * time.Hour).Truncate(time.Microsecond)
	if err := pg.BatchInsert(context.Background(), []audit.Event{record.Event}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Pool.Exec(context.Background(), `UPDATE audit_events SET received_at=NULL WHERE id=$1`, record.Event.ID); err != nil {
		t.Fatal(err)
	}
	return record
}

func TestArchiveJobsFailoverAndLateCommit(t *testing.T) {
	j, pg := jobsFixture(t)
	ctx := context.Background()
	first := insertHistorical(t, j, pg)
	job, err := j.Claim(ctx, "prod")
	if err != nil || job == nil {
		t.Fatal("planning failed", err)
	}
	if other, err := j.Claim(ctx, "prod"); err != nil || other != nil {
		t.Fatal("multiple active archive jobs")
	}
	records, err := j.Load(ctx, job)
	if err != nil || len(records) != 1 || records[0].Event.ID != first.Event.ID || records[0].ReceivedAt != nil {
		t.Fatal("planned membership or historical receipt changed", err)
	}
	objects := &fakeObjects{}
	receipt, err := Export(ctx, objects, job.Environment, job.ID, job.PlannedAt, records)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE audit_archive_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	current, err := j.Claim(ctx, "prod")
	if err != nil || current == nil || current.ID != job.ID || current.Token == job.Token {
		t.Fatal("failed to reclaim same archive job", err)
	}
	if err := j.Complete(ctx, job, receipt); !errors.Is(err, ErrArchiveLeaseLost) {
		t.Fatal("stale worker advanced archive catalog", err)
	}
	if err := j.Complete(ctx, current, receipt); err != nil {
		t.Fatal(err)
	}
	late := sample(t)[0]
	late.Event.CreatedAt = first.Event.CreatedAt.Add(-time.Hour)
	if err := pg.BatchInsert(ctx, []audit.Event{late.Event}); err != nil {
		t.Fatal(err)
	}
	if fresh, err := j.Claim(ctx, "prod"); err != nil || fresh != nil {
		t.Fatal("fresh receipt ignored one-hour archive lag", err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE audit_events SET received_at=clock_timestamp()-interval '2 hours' WHERE id=$1`, late.Event.ID); err != nil {
		t.Fatal(err)
	}
	fresh, err := j.Claim(ctx, "prod")
	if err != nil || fresh == nil || fresh.ID == job.ID {
		t.Fatal("late commit skipped", err)
	}
	freshRecords, err := j.Load(ctx, fresh)
	if err != nil || len(freshRecords) != 1 || freshRecords[0].Event.ID != late.Event.ID {
		t.Fatal("late event not in new membership", err)
	}
	var hot int
	if err := j.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_events`).Scan(&hot); err != nil || hot != 2 {
		t.Fatal("archiving changed hot history", err)
	}
}

func TestArchiveWorkerRetriesSameJobAfterManifestFailure(t *testing.T) {
	j, pg := jobsFixture(t)
	ctx := context.Background()
	insertHistorical(t, j, pg)
	objects := &fakeObjects{failPut: 2}
	worker := &Worker{Jobs: j, Objects: objects, Environment: "prod"}
	if worked, err := worker.Tick(ctx); !worked || err == nil {
		t.Fatal("unverified manifest accepted")
	}
	var pending, jobs, members int
	if err := j.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_archive_jobs WHERE state='planned'`).Scan(&pending); err != nil || pending != 1 {
		t.Fatal("failed upload lost pending job", err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE audit_archive_jobs SET next_attempt=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	objects.failPut = 0
	if worked, err := worker.Tick(ctx); !worked || err != nil {
		t.Fatal("retry failed", err)
	}
	if err := j.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_archive_jobs`).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := j.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_archive_members`).Scan(&members); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || members != 1 {
		t.Fatal("restart duplicated job or event membership")
	}
	if objects.puts[0] != objects.puts[2] {
		t.Fatal("retry changed batch key")
	}
	if worked, err := worker.Tick(ctx); worked || err != nil {
		t.Fatal("verified membership scheduled again", err)
	}
}

func TestCanonicalReceiptSurvivesReplay(t *testing.T) {
	j, pg := jobsFixture(t)
	ctx := context.Background()
	e := sample(t)[0].Event
	if err := pg.BatchInsert(ctx, []audit.Event{e}); err != nil {
		t.Fatal(err)
	}
	var first, again time.Time
	if err := j.Pool.QueryRow(ctx, `SELECT received_at FROM audit_events WHERE id=$1`, e.ID).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := pg.BatchInsert(ctx, []audit.Event{e}); err != nil {
		t.Fatal(err)
	}
	if err := j.Pool.QueryRow(ctx, `SELECT received_at FROM audit_events WHERE id=$1`, e.ID).Scan(&again); err != nil || first.IsZero() || !first.Equal(again) {
		t.Fatal("replay changed canonical receipt", err)
	}
}

package archive

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ledatu/csar-core/pgutil"
)

const archiveDBTimeout = 15 * time.Second

var ErrArchiveLeaseLost = errors.New("archive job lease lost")

type Jobs struct{ Pool *pgxpool.Pool }
type Job struct {
	ID, Environment, Token, Coverage string
	PlannedAt                        time.Time
	Rows                             int
}

// Migrate creates metadata only. Membership protects hot rows from accidental
// deletion; a future partition-retirement migration needs a separate proof.
func (j *Jobs) Migrate(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, archiveDBTimeout)
	defer cancel()
	return pgutil.WithTx(ctx, j.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(839106410824)`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS audit_archive_jobs(
 id uuid PRIMARY KEY,environment text NOT NULL,planned_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 state text NOT NULL DEFAULT 'planned' CHECK(state IN ('planned','catalogued')),
 rows integer NOT NULL CHECK(rows>0 AND rows<=200),coverage_sha256 text NOT NULL,
 lease_token uuid,lease_until timestamptz,next_attempt timestamptz NOT NULL DEFAULT clock_timestamp(),
 receipt jsonb,verified_at timestamptz,
 CHECK((lease_token IS NULL)=(lease_until IS NULL)),
 CHECK((state='catalogued')=(receipt IS NOT NULL AND verified_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS audit_archive_jobs_pending ON audit_archive_jobs(next_attempt,planned_at) WHERE state='planned';
CREATE TABLE IF NOT EXISTS audit_archive_members(
 event_id uuid PRIMARY KEY REFERENCES audit_events(id) ON DELETE RESTRICT,
 job_id uuid NOT NULL REFERENCES audit_archive_jobs(id) ON DELETE RESTRICT
);
CREATE INDEX IF NOT EXISTS audit_archive_members_job ON audit_archive_members(job_id);`)
		return err
	})
}

// Claim serializes planning across replicas. Membership, not a timestamp cursor,
// catches late commits. One live lease coordinates all archive jobs in this DB.
func (j *Jobs) Claim(ctx context.Context, environment string) (*Job, error) {
	if _, err := batchPrefix(environment, uuid.NewString(), time.Now()); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, archiveDBTimeout)
	defer cancel()
	var job *Job
	err := pgutil.WithTx(ctx, j.Pool, func(tx pgx.Tx) error {
		// Advisory-lock wait must not freeze a stale repeatable-read snapshot.
		if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL READ COMMITTED`); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(839106410825)`); err != nil {
			return err
		}
		var active bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM audit_archive_jobs WHERE state='planned' AND lease_until>clock_timestamp())`).Scan(&active); err != nil {
			return err
		}
		if active {
			return nil
		}
		candidate := &Job{Token: uuid.NewString()}
		err := tx.QueryRow(ctx, `SELECT id::text,environment,planned_at,rows,coverage_sha256 FROM audit_archive_jobs
 WHERE state='planned' AND next_attempt<=clock_timestamp() ORDER BY planned_at,id FOR UPDATE LIMIT 1`).Scan(&candidate.ID, &candidate.Environment, &candidate.PlannedAt, &candidate.Rows, &candidate.Coverage)
		switch {
		case err == nil:
			if candidate.Environment != environment {
				return errors.New("pending archive job belongs to another environment")
			}
		case errors.Is(err, pgx.ErrNoRows):
			rows, err := tx.Query(ctx, `WITH candidates AS MATERIALIZED (
 SELECT e.id,e.created_at,6::bigint*octet_length(to_jsonb(e)::text)+1024 AS budget
 FROM audit_events AS e WHERE NOT EXISTS(SELECT 1 FROM audit_archive_members AS m WHERE m.event_id=e.id)
 AND (e.received_at IS NULL OR e.received_at<=clock_timestamp()-interval '1 hour')
 ORDER BY e.created_at,e.id LIMIT 200
), bounded AS (
 SELECT id,sum(budget) OVER(ORDER BY created_at,id) AS cumulative FROM candidates
) SELECT `+recordProjection+` FROM bounded JOIN audit_events AS e ON e.id=bounded.id
 WHERE cumulative<=$1 ORDER BY e.created_at,e.id`, MaxChunkBytes)
			if err != nil {
				return err
			}
			records, err := scanRecords(rows)
			if err != nil {
				return err
			}
			if len(records) == 0 {
				var eligible bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM audit_events e WHERE NOT EXISTS(SELECT 1 FROM audit_archive_members m WHERE m.event_id=e.id) AND (e.received_at IS NULL OR e.received_at<=clock_timestamp()-interval '1 hour'))`).Scan(&eligible); err != nil {
					return err
				}
				if eligible {
					return errors.New("historical audit row exceeds bounded planning budget")
				}
				return nil
			}
			var size int64
			selected := records[:0]
			for i := range records {
				record := &records[i]
				body, err := json.Marshal(record)
				if err != nil {
					return err
				}
				if size+int64(len(body))+1 > MaxChunkBytes {
					break
				}
				size += int64(len(body)) + 1
				selected = append(selected, *record)
			}
			if len(selected) == 0 {
				return errors.New("historical audit row exceeds archive chunk budget")
			}
			description, err := describe(selected)
			if err != nil {
				return err
			}
			candidate.ID = uuid.NewString()
			candidate.Environment = environment
			candidate.Rows = len(selected)
			candidate.Coverage = description.EventIDsSHA256
			if err := tx.QueryRow(ctx, `INSERT INTO audit_archive_jobs(id,environment,rows,coverage_sha256) VALUES($1,$2,$3,$4) RETURNING planned_at`, candidate.ID, environment, candidate.Rows, candidate.Coverage).Scan(&candidate.PlannedAt); err != nil {
				return err
			}
			for i := range selected {
				record := &selected[i]
				if _, err := tx.Exec(ctx, `INSERT INTO audit_archive_members(event_id,job_id) VALUES($1,$2)`, record.Event.ID, candidate.ID); err != nil {
					return err
				}
			}
		default:
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE audit_archive_jobs SET lease_token=$2,lease_until=clock_timestamp()+interval '180 seconds' WHERE id=$1`, candidate.ID, candidate.Token); err != nil {
			return err
		}
		job = candidate
		return nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}

const recordProjection = `e.id::text,e.service,e.actor,e.action,e.target_type,e.target_id,e.scope_type,e.scope_id,e.before_state,e.after_state,e.metadata,e.request_id,e.client_ip,e.created_at,e.received_at`

func scanRecords(rows pgx.Rows) ([]Record, error) {
	defer rows.Close()
	var records []Record
	for rows.Next() {
		var record Record
		e := &record.Event
		if err := rows.Scan(&e.ID, &e.Service, &e.Actor, &e.Action, &e.TargetType, &e.TargetID, &e.ScopeType, &e.ScopeID, &e.BeforeState, &e.AfterState, &e.Metadata, &e.RequestID, &e.ClientIP, &e.CreatedAt, &record.ReceivedAt); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (j *Jobs) Load(ctx context.Context, job *Job) ([]Record, error) {
	ctx, cancel := context.WithTimeout(ctx, archiveDBTimeout)
	defer cancel()
	rows, err := j.Pool.Query(ctx, `SELECT `+recordProjection+` FROM audit_archive_members AS m JOIN audit_events AS e ON e.id=m.event_id
 JOIN audit_archive_jobs AS j ON j.id=m.job_id WHERE j.id=$1 AND j.lease_token=$2 AND j.lease_until>clock_timestamp() AND j.state='planned'
 ORDER BY e.created_at,e.id`, job.ID, job.Token)
	if err != nil {
		return nil, err
	}
	records, err := scanRecords(rows)
	if err != nil {
		return nil, err
	}
	descriptor, err := describe(records)
	if err != nil {
		return nil, err
	}
	if len(records) != job.Rows || descriptor.EventIDsSHA256 != job.Coverage {
		return nil, ErrArchiveLeaseLost
	}
	return records, nil
}

// Complete accepts only verified export receipts for this exact membership.
func (j *Jobs) Complete(ctx context.Context, job *Job, receipt *ExportReceipt) error {
	if receipt == nil {
		return errors.New("verified archive receipt required")
	}
	prefix, err := batchPrefix(job.Environment, job.ID, job.PlannedAt)
	if err != nil {
		return err
	}
	m := &receipt.Manifest
	if m.SchemaVersion != SchemaVersion || m.Chunk.SchemaVersion != SchemaVersion || m.BatchID != job.ID || m.Environment != job.Environment || !m.PlannedAt.Equal(job.PlannedAt) || m.Chunk.Rows != job.Rows || m.Chunk.EventIDsSHA256 != job.Coverage || receipt.ManifestKey != prefix+"manifest.json" || m.DataKey != prefix+"events-0001.ndjson.gz" || !pinnedVersion(m.DataVersionID) || !pinnedVersion(receipt.ManifestVersionID) {
		return errors.New("archive receipt does not cover claimed job")
	}
	body, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, archiveDBTimeout)
	defer cancel()
	tag, err := j.Pool.Exec(ctx, `UPDATE audit_archive_jobs SET state='catalogued',receipt=$3,verified_at=clock_timestamp(),lease_token=NULL,lease_until=NULL
 WHERE id=$1 AND lease_token=$2 AND lease_until>clock_timestamp() AND state='planned'`, job.ID, job.Token, body)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrArchiveLeaseLost
	}
	return nil
}

func (j *Jobs) Retry(ctx context.Context, job *Job) error {
	ctx, cancel := context.WithTimeout(ctx, archiveDBTimeout)
	defer cancel()
	tag, err := j.Pool.Exec(ctx, `UPDATE audit_archive_jobs SET next_attempt=clock_timestamp()+interval '30 seconds',lease_token=NULL,lease_until=NULL
 WHERE id=$1 AND lease_token=$2 AND lease_until>clock_timestamp() AND state='planned'`, job.ID, job.Token)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrArchiveLeaseLost
	}
	return nil
}

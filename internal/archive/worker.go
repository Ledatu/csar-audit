package archive

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Worker struct {
	Jobs        *Jobs
	Objects     Objects
	Environment string
}

// Tick keeps one batch bounded below the 180s lease. A crash after any upload
// leaves the same job/membership for replay; no row-retirement path exists.
func (w *Worker) Tick(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()
	job, err := w.Jobs.Claim(ctx, w.Environment)
	if err != nil || job == nil {
		return false, err
	}
	records, err := w.Jobs.Load(ctx, job)
	if err == nil {
		var receipt *ExportReceipt
		receipt, err = Export(ctx, w.Objects, job.Environment, job.ID, job.PlannedAt, records)
		if err == nil {
			err = w.Jobs.Complete(ctx, job, receipt)
		}
	}
	if err != nil {
		// Failure/cancellation may prevent releasing a lease; expiry permits replay.
		_ = w.Jobs.Retry(ctx, job)
		return true, err
	}
	return true, nil
}

func (w *Worker) Run(ctx context.Context, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	for ctx.Err() == nil {
		worked, err := w.Tick(ctx)
		if err != nil && ctx.Err() == nil {
			logger.Warn("audit archive failed; job and hot history retained")
		}
		wait := 30 * time.Second
		if worked && err == nil {
			wait = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

type archiveCollector struct {
	jobs                *Jobs
	count, age, success *prometheus.Desc
}

func (j *Jobs) RegisterMetrics(registry prometheus.Registerer) error {
	return registry.Register(&archiveCollector{jobs: j,
		count:   prometheus.NewDesc("audit_archive_pending_events", "Hot events not yet covered by a verified catalogued manifest", nil, nil),
		age:     prometheus.NewDesc("audit_archive_oldest_age_seconds", "Oldest unarchived receipt age; historical events fall back to occurred time", nil, nil),
		success: prometheus.NewDesc("audit_archive_scrape_success", "Whether archive lag query succeeded", nil, nil)})
}
func (c *archiveCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.count
	ch <- c.age
	ch <- c.success
}
func (c *archiveCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var count int64
	var age float64
	err := c.jobs.Pool.QueryRow(ctx, `SELECT count(*),COALESCE(GREATEST(extract(epoch FROM clock_timestamp()-min(COALESCE(e.received_at,e.created_at))),0),0)::float8
 FROM audit_events e WHERE NOT EXISTS(SELECT 1 FROM audit_archive_members m JOIN audit_archive_jobs j ON j.id=m.job_id WHERE m.event_id=e.id AND j.state='catalogued')`).Scan(&count, &age)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 0)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.success, prometheus.GaugeValue, 1)
	ch <- prometheus.MustNewConstMetric(c.count, prometheus.GaugeValue, float64(count))
	ch <- prometheus.MustNewConstMetric(c.age, prometheus.GaugeValue, age)
}

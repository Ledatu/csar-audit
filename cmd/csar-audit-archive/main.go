// csar-audit-archive verifies pinned logical archives and imports only into a
// specifically named localhost drill database. It never starts runtime services.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ledatu/csar-audit/internal/archive"
	"github.com/ledatu/csar-audit/internal/config"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/audit"
	"github.com/ledatu/csar-core/configutil"
	"github.com/ledatu/csar-core/s3store"
	"gopkg.in/yaml.v3"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout)
	cancel()
	if err != nil {
		// Errors returned by run are safe stage labels, not provider/SQL payloads.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	if len(args) > 0 && args[0] == "replicate" {
		return runReplicate(ctx, args[1:], output)
	}
	if len(args) == 0 || (args[0] != "verify" && args[0] != "restore-local" && args[0] != "probe" && args[0] != "probe-replay") {
		return errors.New("usage: csar-audit-archive {verify|restore-local|probe|probe-replay|replicate}; explicit command configuration and receipt flags required")
	}
	command := args[0]
	if command == "restore-local" {
		if _, err := restoreConfig(os.Getenv("AUDIT_RESTORE_DATABASE_URL")); err != nil {
			return err
		}
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "archive-only YAML; never runtime DSNs")
	receiptPath := flags.String("receipt-file", "", "pinned metadata receipt; probe creates it exclusively")
	if err := flags.Parse(args[1:]); err != nil || flags.NArg() != 0 || *configPath == "" || *receiptPath == "" {
		return errors.New("explicit config and receipt-file required; invalid arguments")
	}
	cfg, err := loadArchiveConfig(*configPath)
	if err != nil {
		return errors.New("archive-only configuration invalid or unreadable")
	}
	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client, err := s3store.NewClient(&s3store.Config{Bucket: cfg.Bucket, Endpoint: cfg.Endpoint, Region: cfg.Region, Prefix: cfg.Prefix, Auth: cfg.Auth}, logger)
	if err != nil {
		return errors.New("archive client configuration invalid")
	}
	defer func() { _ = client.Close() }()
	objects := archive.S3Objects{Client: client}
	var receipt *archive.ExportReceipt
	if command == "probe" {
		if cfg.Environment != "preflight" {
			return errors.New("synthetic probe requires environment preflight")
		}
		file, err := os.OpenFile(*receiptPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return errors.New("probe receipt path must be new and writable")
		}
		defer func() { _ = file.Close() }()
		records, err := syntheticRecords()
		if err != nil {
			return errors.New("synthetic event preparation failed")
		}
		receipt, err = archive.Export(ctx, objects, cfg.Environment, uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond), records)
		if err != nil {
			return errors.New("synthetic export failed; any uploaded objects retained")
		}
		if err := json.NewEncoder(file).Encode(receipt); err != nil {
			return errors.New("probe receipt write failed; uploaded objects retained")
		}
		if err := file.Sync(); err != nil {
			return errors.New("probe receipt sync failed; uploaded objects retained")
		}
	} else {
		receipt, err = loadReceipt(*receiptPath)
		if err != nil {
			return errors.New("pinned receipt invalid or unreadable")
		}
	}
	manifest, records, err := archive.Restore(ctx, objects, receipt.ManifestKey, receipt.ManifestVersionID)
	if err != nil {
		return errors.New("pinned archive verification failed; no rows imported")
	}
	if manifest.Environment != cfg.Environment || !reflect.DeepEqual(*manifest, receipt.Manifest) {
		return errors.New("archive environment or receipt descriptor mismatch; no rows imported")
	}
	if command == "probe-replay" {
		if cfg.Environment != "preflight" {
			return errors.New("synthetic replay requires environment preflight")
		}
		for i := range records {
			if records[i].Event.Service != "csar-audit-preflight" || records[i].Event.Actor != "system:archive-preflight" || records[i].Event.Action != "archive.probe" {
				return errors.New("replay accepts only synthetic probe records")
			}
		}
		replayed, err := archive.Export(ctx, objects, cfg.Environment, manifest.BatchID, manifest.PlannedAt, records)
		if err != nil || replayed.ManifestVersionID != receipt.ManifestVersionID || !reflect.DeepEqual(replayed.Manifest, *manifest) {
			return errors.New("synthetic conditional replay failed or changed versions")
		}
	}
	if command == "restore-local" {
		if err := restoreLocal(ctx, os.Getenv("AUDIT_RESTORE_DATABASE_URL"), records, logger); err != nil {
			return err
		}
	}
	return json.NewEncoder(output).Encode(struct {
		Command  string                 `json:"command"`
		Verified bool                   `json:"verified"`
		Rows     int                    `json:"rows"`
		Receipt  *archive.ExportReceipt `json:"receipt"`
	}{command, true, len(records), receipt})
}

func loadArchiveConfig(path string) (*config.ArchiveConfig, error) {
	data, err := boundedFile(path, 1<<20)
	if err != nil {
		return nil, err
	}
	var root struct {
		Archive config.ArchiveConfig `yaml:"archive"`
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&root); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing configuration")
	}
	configutil.ExpandEnvInStruct(reflect.ValueOf(&root).Elem())
	if err := root.Archive.Validate(); err != nil {
		return nil, err
	}
	return &root.Archive, nil
}

func boundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.Mode().IsRegular() || stat.Size() > limit {
		return nil, errors.New("file type/size rejected")
	}
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("file grew beyond limit")
	}
	return data, nil
}

func loadReceipt(path string) (*archive.ExportReceipt, error) {
	data, err := boundedFile(path, archive.MaxManifestBytes)
	if err != nil {
		return nil, err
	}
	var receipt archive.ExportReceipt
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&receipt); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing receipt")
	}
	return &receipt, nil
}

func restoreConfig(dsn string) (*pgxpool.Config, error) {
	if dsn == "" {
		return nil, errors.New("AUDIT_RESTORE_DATABASE_URL is required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("restore target DSN invalid")
	}
	if cfg.ConnConfig.Database != "csar_audit_restore" || (cfg.ConnConfig.Host != "127.0.0.1" && cfg.ConnConfig.Host != "localhost") || len(cfg.ConnConfig.Fallbacks) != 0 {
		return nil, errors.New("restore accepts only localhost database csar_audit_restore without fallbacks; use sslmode=disable")
	}
	// Do not let localhost DNS or PGOPTIONS redirect writes or schema selection.
	cfg.ConnConfig.Host = "127.0.0.1"
	cfg.ConnConfig.RuntimeParams = map[string]string{"search_path": "public", "statement_timeout": "15000", "lock_timeout": "3000", "application_name": "csar-audit-archive-restore"}
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	cfg.MaxConns = 2
	return cfg, nil
}

func restoreLocal(ctx context.Context, dsn string, records []archive.Record, logger *slog.Logger) error {
	cfg, err := restoreConfig(dsn)
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return errors.New("isolated restore pool failed")
	}
	defer pool.Close()
	var database string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&database); err != nil || database != "csar_audit_restore" {
		return errors.New("isolated restore database identity check failed")
	}
	pg := store.NewPostgres(pool, logger)
	if err := pg.Migrate(ctx); err != nil {
		return errors.New("isolated restore migration failed")
	}
	events := make([]audit.Event, len(records))
	receipts := make([]*time.Time, len(records))
	for i := range records {
		events[i] = records[i].Event
		receipts[i] = records[i].ReceivedAt
	}
	if err := pg.RestoreBatch(ctx, events, receipts); err != nil {
		return errors.New("isolated restore import failed; transaction rolled back or commit outcome uncertain")
	}
	return nil
}

func syntheticRecords() ([]archive.Record, error) {
	records := make([]archive.Record, 2)
	for i := range records {
		event, err := audit.PrepareEvent(&audit.Event{Service: "csar-audit-preflight", Actor: "system:archive-preflight", Action: "archive.probe", TargetType: "synthetic", TargetID: uuid.NewString(), ScopeType: "platform"})
		if err != nil {
			return nil, err
		}
		records[i].Event = *event
	}
	received := time.Now().UTC().Truncate(time.Microsecond)
	records[0].ReceivedAt = &received
	return records, nil
}

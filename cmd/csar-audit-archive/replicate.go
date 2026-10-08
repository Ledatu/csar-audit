package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/ledatu/csar-audit/internal/archive"
	"github.com/ledatu/csar-audit/internal/config"
	"github.com/ledatu/csar-core/s3store"
)

func runReplicate(ctx context.Context, args []string, output io.Writer) error {
	flags := flag.NewFlagSet("replicate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	sourcePath := flags.String("source-config", "", "archive-only source reader config")
	destinationPath := flags.String("destination-config", "", "archive-only destination writer config")
	receiptPath := flags.String("receipt-file", "", "pinned source receipt")
	outputPath := flags.String("output-receipt", "", "new exclusive destination receipt")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *sourcePath == "" || *destinationPath == "" || *receiptPath == "" || *outputPath == "" {
		return errors.New("replicate requires explicit source-config, destination-config, receipt-file and output-receipt; invalid arguments")
	}
	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()
	sourceCfg, err := loadArchiveConfig(*sourcePath)
	if err != nil {
		return errors.New("replica source archive-only configuration invalid or unreadable")
	}
	destinationCfg, err := loadArchiveConfig(*destinationPath)
	if err != nil {
		return errors.New("replica destination archive-only configuration invalid or unreadable")
	}
	location := func(cfg *config.ArchiveConfig) archive.ReplicaLocation {
		return archive.ReplicaLocation{Endpoint: cfg.Endpoint, Bucket: cfg.Bucket, Prefix: cfg.Prefix, Environment: cfg.Environment}
	}
	from, to := location(sourceCfg), location(destinationCfg)
	if err := archive.ValidateReplicaLocations(from, to); err != nil {
		return err
	}
	receipt, err := loadReceipt(*receiptPath)
	if err != nil || receipt.Manifest.Environment != from.Environment {
		return errors.New("replica source receipt invalid or environment mismatch")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := func(cfg *config.ArchiveConfig) (*s3store.Client, error) {
		return s3store.NewClient(&s3store.Config{Bucket: cfg.Bucket, Endpoint: cfg.Endpoint, Region: cfg.Region, Prefix: cfg.Prefix, Auth: cfg.Auth}, logger)
	}
	source, err := client(sourceCfg)
	if err != nil {
		return errors.New("replica source client configuration invalid")
	}
	defer func() { _ = source.Close() }()
	destination, err := client(destinationCfg)
	if err != nil {
		return errors.New("replica destination client configuration invalid")
	}
	defer func() { _ = destination.Close() }()
	file, err := os.OpenFile(*outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("replica output receipt path must be new and writable")
	}
	defer func() { _ = file.Close() }()
	copied, err := archive.Replicate(ctx, archive.S3Objects{Client: source}, archive.S3Objects{Client: destination}, from, to, receipt)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(file).Encode(copied); err != nil {
		return errors.New("replica receipt write failed; destination objects retained")
	}
	if err := file.Sync(); err != nil {
		return errors.New("replica receipt sync failed; destination objects retained")
	}
	if err := file.Close(); err != nil {
		return errors.New("replica receipt close failed; destination objects retained")
	}
	if err := json.NewEncoder(output).Encode(struct {
		Command  string                 `json:"command"`
		Verified bool                   `json:"verified"`
		Rows     int                    `json:"rows"`
		Receipt  *archive.ExportReceipt `json:"receipt"`
	}{"replicate", true, copied.Manifest.Chunk.Rows, copied}); err != nil {
		return errors.New("replica report write failed; destination receipt retained")
	}
	return nil
}

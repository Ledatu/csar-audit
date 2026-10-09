package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ledatu/csar-audit/internal/config"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/pgutil"
)

func openPostgres(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*pgxpool.Pool, *store.Postgres, error) {
	if cfg.Database.IngestOnly {
		return nil, nil, nil
	}
	pool, err := pgutil.NewPool(ctx, cfg.Database.DSN, pgutil.WithLogger(logger.With("component", "postgres")))
	if err != nil {
		return nil, nil, fmt.Errorf("postgres: %w", err)
	}
	pgStore := store.NewPostgres(pool, logger.With("component", "audit_store"))
	if err := pgStore.Migrate(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("migrate: %w", err)
	}
	return pool, pgStore, nil
}

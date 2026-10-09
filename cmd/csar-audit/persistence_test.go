package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ledatu/csar-audit/internal/config"
	"github.com/ledatu/csar-audit/internal/query"
	"github.com/ledatu/csar-core/gatewayctx"
)

func TestIngestOnlyNeverInitializesPostgres(t *testing.T) {
	cfg := &config.Config{Database: config.DatabaseConfig{DSN: "invalid DSN", IngestOnly: true}}
	pool, st, err := openPostgres(context.Background(), cfg, slog.Default())
	if err != nil || pool != nil || st != nil {
		t.Fatalf("ingest-only opened persistence: pool=%v store=%v err=%v", pool, st, err)
	}
	cfg.Database.IngestOnly = false
	if _, _, err := openPostgres(context.Background(), cfg, slog.Default()); err == nil {
		t.Fatal("normal mode silently bypassed invalid PostgreSQL configuration")
	}
}

func TestIngestOnlyQueriesUnavailableAndStillRequireIdentity(t *testing.T) {
	mux := http.NewServeMux()
	query.New(nil).Register(mux)
	for _, path := range []string{"/admin/audit", "/admin/audit/", "/admin/audit/groups", "/admin/audit/groups/"} {
		for _, authenticated := range []bool{false, true} {
			r := httptest.NewRequest(http.MethodGet, path, nil)
			want := http.StatusUnauthorized
			if authenticated {
				r = r.WithContext(gatewayctx.NewContext(r.Context(), &gatewayctx.Identity{Subject: "operator"}))
				want = http.StatusServiceUnavailable
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != want {
				t.Fatalf("path=%s authenticated=%v status=%d want=%d", path, authenticated, w.Code, want)
			}
		}
	}
}

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/ledatu/csar-core/audit"
)

// Tests require an explicitly named local disposable database. Each test uses
// its own schema and never runs migrations against an application's tables.
func integrationStore(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("AUDIT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AUDIT_TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Database != "csar_audit_test" || (cfg.ConnConfig.Host != "127.0.0.1" && cfg.ConnConfig.Host != "localhost") {
		t.Fatal("integration tests require localhost and database csar_audit_test")
	}
	ctx := context.Background()
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{fmt.Sprintf("audit_identity_%d", time.Now().UnixNano())}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("cleanup isolated schema: %v", err)
		}
		admin.Close()
	})
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	// Exercise independence from a caller's default transaction isolation.
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
	cfg.MaxConns = 8
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := NewPostgres(pool, nil)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s
}

func integrationEvents(t *testing.T, count int) []audit.Event {
	t.Helper()
	events := make([]audit.Event, count)
	for i := range events {
		event, err := audit.PrepareEvent(&audit.Event{
			Service: "test", Actor: "user", Action: "campaign.update", TargetType: "campaign",
			TargetID: "test", ScopeType: "tenant", ScopeID: "test",
			BeforeState: json.RawMessage(`{"a":1}`), AfterState: json.RawMessage(`{"a":2}`),
			Metadata: json.RawMessage(`{"a":1,"b":2}`), RequestID: "test-request", ClientIP: "127.0.0.1",
		})
		if err != nil {
			t.Fatal(err)
		}
		events[i] = *event
	}
	return events
}

func eventCount(t *testing.T, s *Postgres) int {
	t.Helper()
	var count int
	if err := s.pool.QueryRow(context.Background(), "SELECT count(*) FROM audit_events").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestPostgresIntegrationExactReplayAndConflict(t *testing.T) {
	for _, size := range []int{2, copyFromThreshold + 1} {
		t.Run(fmt.Sprintf("batch_%d", size), func(t *testing.T) {
			s := integrationStore(t)
			ctx := context.Background()
			events := integrationEvents(t, size)
			// Duplicate IDs within the same delivery must not fail COPY or INSERT.
			batch := append(append([]audit.Event(nil), events...), events[0])
			if err := s.BatchInsert(ctx, batch); err != nil {
				t.Fatal(err)
			}
			if err := s.BatchInsert(ctx, events); err != nil {
				t.Fatal(err)
			}
			if got := eventCount(t, s); got != size {
				t.Fatalf("replay inserted duplicates: %d", got)
			}
			// Equivalent JSON syntax and UUID spelling still identify a replay.
			events[0].Metadata = json.RawMessage(`{ "b": 2, "a": 1 }`)
			events[0].ID = strings.ToUpper(events[0].ID)
			if err := s.BatchInsert(ctx, events); err != nil {
				t.Fatalf("semantic JSON replay: %v", err)
			}
			if got := eventCount(t, s); got != size {
				t.Fatalf("JSON replay inserted duplicates: %d", got)
			}
			// An unrelated new row must roll back alongside a conflicting replay.
			newEvents := integrationEvents(t, size)
			conflict := events[0]
			conflict.Action = "campaign.delete"
			err := s.BatchInsert(ctx, append(newEvents, conflict))
			var conflictErr *EventConflictError
			canonicalID := strings.ToLower(events[0].ID)
			if !errors.As(err, &conflictErr) || conflictErr.ID != canonicalID {
				t.Fatalf("expected identified payload conflict: %v", err)
			}
			if got := eventCount(t, s); got != size {
				t.Fatalf("conflicting batch partially committed: %d", got)
			}
			listed, err := s.List(ctx, &ListFilter{Limit: 100})
			if err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, event := range listed.Events {
				if event.ID == canonicalID {
					found = true
					if event.Action != "campaign.update" || !event.CreatedAt.Equal(events[0].CreatedAt) {
						t.Fatal("replay overwrote original contents or timestamp")
					}
				}
			}
			if !found {
				t.Fatal("producer ID not preserved in query response")
			}
		})
	}
}

func TestPostgresIntegrationConflictingIDsWithinBatch(t *testing.T) {
	for _, size := range []int{2, copyFromThreshold + 1} {
		t.Run(fmt.Sprintf("batch_%d", size), func(t *testing.T) {
			s := integrationStore(t)
			events := integrationEvents(t, size)
			conflict := events[0]
			conflict.Metadata = json.RawMessage(`{"different":true}`)
			var conflictErr *EventConflictError
			if err := s.BatchInsert(context.Background(), append(events, conflict)); !errors.As(err, &conflictErr) {
				t.Fatalf("expected in-batch conflict: %v", err)
			}
			if got := eventCount(t, s); got != 0 {
				t.Fatalf("conflicting new batch committed %d rows", got)
			}
		})
	}
}

func TestPostgresIntegrationConcurrentReplay(t *testing.T) {
	s := integrationStore(t)
	events := integrationEvents(t, copyFromThreshold+1)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	errorsCh := make(chan error, 8)
	var workers sync.WaitGroup
	for i := range 8 {
		workers.Go(func() {
			batch := append([]audit.Event(nil), events...)
			if i%2 == 1 {
				for left, right := 0, len(batch)-1; left < right; left, right = left+1, right-1 {
					batch[left], batch[right] = batch[right], batch[left]
				}
			}
			errorsCh <- s.BatchInsert(ctx, batch)
		})
	}
	workers.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := eventCount(t, s); got != len(events) {
		t.Fatalf("concurrent delivery duplicated events: %d", got)
	}
}

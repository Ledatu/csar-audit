package store

import (
	"context"
	"fmt"
	"testing"
)

func TestPostgresIntegrationStagingSurvivesAdditiveDefaultColumn(t *testing.T) {
	for _, size := range []int{1, copyFromThreshold + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s := integrationStore(t)
			ctx := context.Background()
			// A newer writer adds a column that this binary does not supply.
			// It is compatible at the destination because it has a default;
			// staging must not copy its NOT NULL constraint without that default.
			if _, err := s.pool.Exec(ctx, `ALTER TABLE audit_events
				ADD COLUMN future_context text NOT NULL DEFAULT ''`); err != nil {
				t.Fatal(err)
			}
			events := integrationEvents(t, size)
			for range 2 {
				if err := s.BatchInsert(ctx, events); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM audit_events
				WHERE future_context = ''`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != size || eventCount(t, s) != size {
				t.Fatal("additive default or exact replay was lost")
			}
		})
	}
}

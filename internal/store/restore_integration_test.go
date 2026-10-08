package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ledatu/csar-core/audit"
)

func TestRestorePreservesCanonicalReceiptsAndRejectsConflicts(t *testing.T) {
	for _, size := range []int{2, copyFromThreshold + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s := integrationStore(t)
			ctx := context.Background()
			events := integrationEvents(t, size)
			receipts := make([]*time.Time, size)
			known := time.Now().UTC().Add(-48 * time.Hour).Truncate(time.Microsecond)
			receipts[0] = &known
			for attempt := 0; attempt < 2; attempt++ {
				if err := s.RestoreBatch(ctx, events, receipts); err != nil {
					t.Fatal(err)
				}
			}
			if eventCount(t, s) != size {
				t.Fatal("restore replay duplicated rows")
			}
			for i := range events {
				var got *time.Time
				if err := s.pool.QueryRow(ctx, "SELECT received_at FROM audit_events WHERE id=$1", events[i].ID).Scan(&got); err != nil {
					t.Fatal(err)
				}
				if (got == nil) != (receipts[i] == nil) || (got != nil && !got.Equal(*receipts[i])) {
					t.Fatal("canonical receipt changed")
				}
			}
			// One receipt conflict rolls back unrelated new events in the same import.
			extra := integrationEvents(t, 1)[0]
			receipts[0] = nil
			err := s.RestoreBatch(ctx, append(events, extra), append(receipts, nil))
			var conflict *EventConflictError
			if !errors.As(err, &conflict) || eventCount(t, s) != size {
				t.Fatal("receipt conflict was overwritten or partly imported", err)
			}
		})
	}
}

func TestRestoreRejectsMissingIdentityAndPrecisionLoss(t *testing.T) {
	s := integrationStore(t)
	events := integrationEvents(t, 1)
	if err := s.RestoreBatch(context.Background(), events, nil); err == nil {
		t.Fatal("missing receipt vector accepted")
	}
	missing := events[0]
	missing.ID = ""
	if err := s.RestoreBatch(context.Background(), []audit.Event{missing}, []*time.Time{nil}); err == nil {
		t.Fatal("restore generated replacement identity")
	}
	noncanonical := events[0]
	noncanonical.ID = strings.ToUpper(noncanonical.ID)
	if err := s.RestoreBatch(context.Background(), []audit.Event{noncanonical}, []*time.Time{nil}); err == nil {
		t.Fatal("restore silently rewrote a noncanonical identity")
	}
	if err := s.RestoreBatch(context.Background(), append(events, events[0]), []*time.Time{nil, nil}); err == nil {
		t.Fatal("duplicate restore identity accepted")
	}
	precise := time.Now().UTC().Truncate(time.Microsecond).Add(time.Nanosecond)
	if err := s.RestoreBatch(context.Background(), events, []*time.Time{&precise}); err == nil {
		t.Fatal("receipt precision silently lost")
	}
	if eventCount(t, s) != 0 {
		t.Fatal("invalid restore wrote events")
	}
}

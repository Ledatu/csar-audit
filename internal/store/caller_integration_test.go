package store

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/ledatu/csar-core/audit"
	"github.com/ledatu/csar-core/caller"
)

func TestCallerContextInsertCopyReplayAndQuery(t *testing.T) {
	for _, size := range []int{1, copyFromThreshold + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s := integrationStore(t)
			ctx := context.Background()
			events := integrationEvents(t, size)
			events[0].Caller = &caller.Identity{Version: caller.Version, CallerKind: caller.Service, CallerID: "svc:reporting",
				EffectiveSubject: "svc:reporting", CredentialKind: caller.STS, Assurance: caller.Verified}
			events[0].OriginCaller = &caller.Identity{Version: caller.Version, CallerKind: caller.Agent, CallerID: "agent:1",
				OwnerSubject: "user:1", EffectiveSubject: "user:1", CredentialKind: caller.APIKey, CredentialID: "key:1", Assurance: caller.Verified}
			events[0].OperationID, events[0].ParentOperationID, events[0].TraceID = "op:1", "op:0", "trace:1"
			for range 2 {
				if err := s.BatchInsert(ctx, events); err != nil {
					t.Fatal(err)
				}
			}
			page, err := s.List(ctx, &ListFilter{Limit: 100})
			if err != nil || len(page.Events) != size {
				t.Fatalf("query failed: %v", err)
			}
			found := false
			for _, row := range page.Events {
				if row.ID != events[0].ID {
					if row.Caller != nil || row.OriginCaller != nil {
						t.Fatal("legacy row acquired caller identity")
					}
					continue
				}
				found = true
				if !reflect.DeepEqual(row.Caller, events[0].Caller) || !reflect.DeepEqual(row.OriginCaller, events[0].OriginCaller) ||
					row.OperationID != "op:1" || row.ParentOperationID != "op:0" || row.TraceID != "trace:1" || row.Actor != events[0].Actor {
					t.Fatal("query lost caller context or changed actor")
				}
			}
			if !found || eventCount(t, s) != size {
				t.Fatal("replay changed row identity/count")
			}
			events[0].OriginCaller.CredentialID = "key:changed"
			var conflict *EventConflictError
			if err := s.BatchInsert(ctx, events); !errors.As(err, &conflict) {
				t.Fatalf("changed caller accepted as replay: %v", err)
			}
		})
	}
}

func TestMalformedCallerNeverReachesStorage(t *testing.T) {
	s := &Postgres{}
	err := s.BatchInsert(context.Background(), []audit.Event{{Actor: "user", Action: "read", TargetType: "report", ScopeType: "tenant",
		Caller: &caller.Identity{Version: 2}}})
	if err == nil {
		t.Fatal("unsupported identity reached storage")
	}
}

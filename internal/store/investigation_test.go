package store

import (
	"context"
	"encoding/json"
	"github.com/ledatu/csar-core/caller"
	"strings"
	"testing"
	"time"
)

func TestInvestigationValidationAndWindow(t *testing.T) {
	now := time.Now().UTC()
	legacy := &ListFilter{Actor: "old"}
	if err := legacy.ValidateInvestigation(now); err != nil || legacy.Since != nil || legacy.Until != nil {
		t.Fatal("legacy window changed", err)
	}
	f := &ListFilter{Initiator: IdentityFilter{Kind: "unknown"}}
	if err := f.ValidateInvestigation(now); err != nil || !f.Until.Equal(now) || f.Until.Sub(*f.Since) != 24*time.Hour {
		t.Fatal("default window", err)
	}
	for _, bad := range []IdentityFilter{{Kind: "robot"}, {CredentialKind: "mcp"}, {Assurance: "guessed"}, {Version: "0"}, {Version: "1 OR 1=1"}, {ClientID: strings.Repeat("a", 513)}, {ConnectionID: "bad\nvalue"}} {
		if (&ListFilter{Caller: bad}).ValidateInvestigation(now) == nil {
			t.Fatalf("accepted invalid filter: %+v", bad)
		}
	}
	since := now.Add(-32 * 24 * time.Hour)
	until := now
	if (&ListFilter{TraceID: "trace", Since: &since, Until: &until}).ValidateInvestigation(now) == nil {
		t.Fatal("accepted unbounded investigation")
	}
}

func TestInvestigationPredicatesAreParameterized(t *testing.T) {
	value := "value' OR true --"
	f := &ListFilter{Caller: IdentityFilter{ID: value}, OriginCaller: IdentityFilter{CredentialID: "key"}, ReleaseEventID: "receipt"}
	where, args := buildWhere(f, false)
	if strings.Contains(where, value) || len(args) != 3 || args[0] != value || !strings.Contains(where, "id::text = $3 OR metadata->>'release_event_id' = $3") {
		t.Fatalf("unsafe/different predicate: %s %#v", where, args)
	}
}

func TestInvestigationNativeIdentityAndGroupParity(t *testing.T) {
	s := integrationStore(t)
	ctx := context.Background()
	events := integrationEvents(t, 6)
	service := &caller.Identity{Version: 1, CallerKind: caller.Service, CallerID: "svc:api", EffectiveSubject: "svc:api", CredentialKind: caller.STS, Assurance: caller.Verified}
	agent := &caller.Identity{Version: 1, CallerKind: caller.Agent, CallerID: "agent:1", OwnerSubject: "owner", EffectiveSubject: "owner", CredentialKind: caller.APIKey, CredentialID: "key:1", ClientID: "client:1", ConnectionID: "connection:1", Assurance: caller.Verified}
	unknown := &caller.Identity{Version: 1, CallerKind: caller.Unknown, EffectiveSubject: "owner", OwnerSubject: "owner", CredentialKind: caller.APIKey, CredentialID: "legacy:key", Assurance: caller.Legacy}
	events[0].Caller, events[0].OriginCaller = service, agent
	events[0].OperationID, events[0].ParentOperationID, events[0].TraceID = "operation:1", "root:1", "trace:1"
	events[0].Metadata = json.RawMessage(`{"http_status":200,"cache_status":"HIT","release_event_id":"` + events[4].ID + `"}`)
	events[1].Caller = &caller.Identity{Version: 1, CallerKind: caller.Human, CallerID: "human", EffectiveSubject: "human", OwnerSubject: "human", CredentialKind: caller.Session, Assurance: caller.Verified}
	events[2].Caller = unknown
	events[3].Actor = "svc:legacy-must-not-be-classified"
	events[4].Caller, events[4].OriginCaller = service, agent
	events[4].OperationID, events[4].ParentOperationID, events[4].TraceID = "operation:1", "root:1", "trace:1"
	events[4].Action = "read.release_authorized"
	events[4].Metadata = json.RawMessage(`{"phase":"release_authorized"}`)
	events[5].Caller, events[5].OriginCaller = service, unknown
	if err := s.BatchInsert(ctx, events); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		filter ListFilter
		want   int
	}{
		{"initiator agent", ListFilter{Initiator: IdentityFilter{Kind: "agent"}}, 2},
		{"immediate service", ListFilter{Caller: IdentityFilter{Kind: "service"}}, 3},
		{"unknown is not missing", ListFilter{Initiator: IdentityFilter{Kind: "unknown"}}, 2},
		{"missing is not inferred", ListFilter{Initiator: IdentityFilter{Kind: "absent"}}, 1},
		{"origin absent", ListFilter{OriginCaller: IdentityFilter{Kind: "absent"}}, 3},
		{"all initiator fields", ListFilter{Initiator: IdentityFilter{Version: "1", Kind: "agent", ID: "agent:1", OwnerSubject: "owner", EffectiveSubject: "owner", CredentialKind: "api_key", CredentialID: "key:1", ClientID: "client:1", ConnectionID: "connection:1", Assurance: "verified"}}, 2},
		{"caller and origin", ListFilter{Caller: IdentityFilter{ID: "svc:api"}, OriginCaller: IdentityFilter{ID: "agent:1"}}, 2},
		{"operation", ListFilter{OperationID: "operation:1"}, 2},
		{"parent", ListFilter{ParentOperationID: "root:1"}, 2},
		{"trace", ListFilter{TraceID: "trace:1"}, 2},
		{"receipt and final", ListFilter{ReleaseEventID: events[4].ID}, 2},
		{"cache", ListFilter{CacheStatus: "HIT"}, 1},
		{"phase", ListFilter{AuditPhase: "release_authorized"}, 1},
		{"event", ListFilter{EventID: events[3].ID}, 1},
		{"no injection", ListFilter{Caller: IdentityFilter{ID: "svc:api' OR true --"}}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page, err := s.List(ctx, &tc.filter)
			if err != nil || len(page.Events) != tc.want {
				t.Fatalf("list: count=%v err=%v", page, err)
			}
			groups, err := s.ListGroups(ctx, &tc.filter)
			if err != nil {
				t.Fatal(err)
			}
			var count int64
			for _, g := range groups.Groups {
				count += g.Count
			}
			if count != int64(tc.want) {
				t.Fatalf("groups count=%d want=%d", count, tc.want)
			}
		})
	}
	f := &ListFilter{Initiator: IdentityFilter{Kind: "agent"}, Limit: 1}
	first, err := s.List(ctx, f)
	if err != nil || first.NextCursor == "" {
		t.Fatal("cursor", err)
	}
	f.Cursor = first.NextCursor
	next, err := s.List(ctx, f)
	if err != nil || len(next.Events) != 1 || next.Events[0].ID == first.Events[0].ID {
		t.Fatal("filtered pagination", err)
	}
}

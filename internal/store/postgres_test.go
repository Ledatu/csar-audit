package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ledatu/csar-core/audit"
)

func TestPostgres_BatchInsert_Empty(t *testing.T) {
	s := &Postgres{}
	if err := s.BatchInsert(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.BatchInsert(context.Background(), []audit.Event{}); err != nil {
		t.Fatal(err)
	}
}

func TestBuildWhere_CategoryAndExclusions(t *testing.T) {
	where, args := buildWhere(&audit.ListFilter{
		Category:          "business_mutation",
		ExcludeCategories: []string{"sensitive_read, router_access", "router_access"},
		ExcludeServices:   []string{"csar-router"},
	}, false)

	if !strings.Contains(where, auditCategoryExpr+" = $1") {
		t.Fatalf("where does not filter category: %s", where)
	}
	if !strings.Contains(where, "NOT IN ($2, $3)") {
		t.Fatalf("where does not exclude categories: %s", where)
	}
	if !strings.Contains(where, "service NOT IN ($4)") {
		t.Fatalf("where does not exclude services: %s", where)
	}
	if len(args) != 4 {
		t.Fatalf("args len = %d, want 4: %#v", len(args), args)
	}
}

func TestBuildWhere_CursorOnlyWhenRequested(t *testing.T) {
	cursor := audit.EncodeListCursor(time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC), "00000000-0000-0000-0000-000000000001")

	withoutCursor, withoutArgs := buildWhere(&audit.ListFilter{Cursor: cursor}, false)
	if strings.Contains(withoutCursor, "created_at <") || len(withoutArgs) != 0 {
		t.Fatalf("group where should ignore cursor: where=%s args=%#v", withoutCursor, withoutArgs)
	}

	withCursor, withArgs := buildWhere(&audit.ListFilter{Cursor: cursor}, true)
	if !strings.Contains(withCursor, "created_at < $1") || len(withArgs) != 2 {
		t.Fatalf("list where should include cursor: where=%s args=%#v", withCursor, withArgs)
	}
}

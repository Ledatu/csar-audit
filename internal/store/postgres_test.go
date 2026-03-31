package store

import (
	"context"
	"testing"

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

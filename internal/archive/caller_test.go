package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ledatu/csar-core/audit"
	"github.com/ledatu/csar-core/caller"
)

func TestLegacyPinnedPayloadBytesRemainSchemaOne(t *testing.T) {
	const id = "c30ca535-d731-4559-9238-d2b6906f1129"
	const legacy = `{"event":{"id":"c30ca535-d731-4559-9238-d2b6906f1129","actor":"user","action":"read","target_type":"report","target_id":"","scope_type":"tenant","scope_id":"","created_at":"2026-11-21T00:00:00Z"}}` + "\n"
	var body bytes.Buffer
	w, err := gzip.NewWriterLevel(&body, gzip.BestSpeed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(legacy)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	descriptor := Chunk{SchemaVersion: 1, Rows: 1, CompressedBytes: int64(body.Len()), UncompressedBytes: int64(len(legacy)),
		CompressedSHA256: digest(body.Bytes()), UncompressedSHA256: digest([]byte(legacy)),
		EventIDsSHA256: digest([]byte(id + "\n")), OccurredMin: ts, OccurredMax: ts, UnknownReceiptRows: 1}
	records, err := Verify(context.Background(), bytes.NewReader(body.Bytes()), &descriptor)
	if err != nil || records[0].Event.Caller != nil {
		t.Fatal("historical payload changed or acquired identity", err)
	}
	var rewritten bytes.Buffer
	actual, err := Encode(context.Background(), &rewritten, records)
	if err != nil || actual.SchemaVersion != 1 || !bytes.Equal(body.Bytes(), rewritten.Bytes()) {
		t.Fatal("historical bytes/checksums were rewritten", err)
	}
}

func TestCallerArchiveSQLExportRestoreAndReplay(t *testing.T) {
	j, pg := jobsFixture(t)
	ctx := context.Background()
	record := sample(t)[0]
	record.Event.Caller = &caller.Identity{Version: caller.Version, CallerKind: caller.Agent, CallerID: "agent:1",
		OwnerSubject: "user:1", EffectiveSubject: "user:1", CredentialKind: caller.APIKey, CredentialID: "key:1", Assurance: caller.Verified}
	record.Event.OperationID = "op:1"
	if err := pg.BatchInsert(ctx, []audit.Event{record.Event}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Pool.Exec(ctx, `UPDATE audit_events SET received_at=NULL WHERE id=$1`, record.Event.ID); err != nil {
		t.Fatal(err)
	}
	job, err := j.Claim(ctx, "test")
	if err != nil || job == nil {
		t.Fatal("archive planning failed", err)
	}
	records, err := j.Load(ctx, job)
	if err != nil || len(records) != 1 || !reflect.DeepEqual(records[0].Event.Caller, record.Event.Caller) {
		t.Fatal("archive SQL projection dropped identity", err)
	}
	objects := &fakeObjects{}
	receipt, err := Export(ctx, objects, "test", job.ID, job.PlannedAt, records)
	if err != nil || receipt.Manifest.SchemaVersion != 2 {
		t.Fatal("caller archive was not versioned", err)
	}
	_, restored, err := Restore(ctx, objects, receipt.ManifestKey, receipt.ManifestVersionID)
	if err != nil || !reflect.DeepEqual(restored[0].Event.Caller, record.Event.Caller) || restored[0].Event.OperationID != "op:1" {
		t.Fatal("caller context lost on restore", err)
	}
	if err := pg.RestoreBatch(ctx, []audit.Event{restored[0].Event}, []*time.Time{restored[0].ReceivedAt}); err != nil {
		t.Fatal("restored event was not an exact replay", err)
	}
	if err := j.Complete(ctx, job, receipt); err != nil {
		t.Fatal("schema 2 archive could not be catalogued", err)
	}
}

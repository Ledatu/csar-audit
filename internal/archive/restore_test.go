package archive

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestRestorePinnedBatchPreservesIdentity(t *testing.T) {
	objects := &fakeObjects{}
	records := sample(t)
	receipt, err := Export(context.Background(), objects, "prod", uuid.NewString(), time.Now().UTC(), records)
	if err != nil {
		t.Fatal(err)
	}
	// A later object at the same key must not change what this manifest restores.
	objects.versions[receipt.Manifest.DataKey+"@latest"] = []byte("newer unrelated body")
	manifest, restored, err := Restore(context.Background(), objects, receipt.ManifestKey, receipt.ManifestVersionID)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.BatchID != receipt.Manifest.BatchID || len(restored) != len(records) {
		t.Fatal("manifest/row coverage changed")
	}
	for i := range records {
		if restored[i].Event.ID != records[i].Event.ID || !restored[i].Event.CreatedAt.Equal(records[i].Event.CreatedAt) {
			t.Fatal("restore changed event identity")
		}
	}
}

func TestRestoreRejectsCorruptionAndForeignReferences(t *testing.T) {
	for _, kind := range []string{"corrupt data", "foreign key", "wrong namespace", "unknown field", "missing version"} {
		t.Run(kind, func(t *testing.T) {
			objects := &fakeObjects{}
			r, err := Export(context.Background(), objects, "prod", uuid.NewString(), time.Now().UTC(), sample(t))
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "corrupt data":
				objects.corruptData = true
			case "foreign key":
				r.Manifest.DataKey = "tokens/credential"
			case "wrong namespace":
				r.Manifest.Environment = "other"
			case "missing version":
				r.Manifest.DataVersionID = "null"
			}
			body, err := json.Marshal(r.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "unknown field" {
				body = append(body[:len(body)-1], []byte(`,"unsupported":true}`)...)
			}
			objects.versions[r.ManifestKey+"@"+r.ManifestVersionID] = body
			manifest, records, err := Restore(context.Background(), objects, r.ManifestKey, r.ManifestVersionID)
			if err == nil || manifest != nil || records != nil {
				t.Fatal("partial/unverified restore became importable")
			}
			if kind != "corrupt data" && len(objects.opens) != 3 {
				t.Fatal("foreign/unvalidated data object was fetched")
			}
		})
	}
}

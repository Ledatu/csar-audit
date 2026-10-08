package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ledatu/csar-core/audit"
)

func sample(t *testing.T) []Record {
	t.Helper()
	event, err := audit.PrepareEvent(&audit.Event{Actor: "user", Action: "update", TargetType: "campaign", ScopeType: "tenant", Metadata: json.RawMessage(`{"x":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	received := time.Now().UTC().Truncate(time.Microsecond)
	return []Record{{Event: *event, ReceivedAt: &received}}
}

func TestDeterministicChunkRoundTrip(t *testing.T) {
	records := sample(t)
	var one, two bytes.Buffer
	d, err := Encode(context.Background(), &one, records)
	if err != nil {
		t.Fatal(err)
	}
	other, err := Encode(context.Background(), &two, records)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(one.Bytes(), two.Bytes()) || d.CompressedSHA256 != other.CompressedSHA256 {
		t.Fatal("retry changed archive content")
	}
	verified, err := Verify(context.Background(), bytes.NewReader(one.Bytes()), &d)
	if err != nil {
		t.Fatal(err)
	}
	if len(verified) != 1 || verified[0].Event.ID != records[0].Event.ID || !verified[0].ReceivedAt.Equal(*records[0].ReceivedAt) {
		t.Fatal("canonical identity/receipt lost")
	}
	records[0].ReceivedAt = nil
	var legacy bytes.Buffer
	ld, err := Encode(context.Background(), &legacy, records)
	if err != nil {
		t.Fatal(err)
	}
	if ld.UnknownReceiptRows != 1 || ld.ReceivedMin != nil {
		t.Fatal("fabricated legacy receipt time")
	}
	if _, err := Verify(context.Background(), bytes.NewReader(legacy.Bytes()), &ld); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptChunkNeverReturnsImportablePrefix(t *testing.T) {
	var body bytes.Buffer
	descriptor, err := Encode(context.Background(), &body, sample(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Chunk, []byte) []byte{
		func(_ *Chunk, b []byte) []byte { b[len(b)/2] ^= 1; return b },
		func(_ *Chunk, b []byte) []byte { return b[:len(b)-1] },
		func(d *Chunk, b []byte) []byte { d.Rows++; return b },
		func(d *Chunk, b []byte) []byte { d.EventIDsSHA256 = "bad"; return b },
		func(d *Chunk, b []byte) []byte { d.UncompressedBytes = MaxChunkBytes + 1; return b },
		func(d *Chunk, b []byte) []byte { d.SchemaVersion++; return b },
	} {
		d := descriptor
		changed := mutate(&d, bytes.Clone(body.Bytes()))
		result, err := Verify(context.Background(), bytes.NewReader(changed), &d)
		if err == nil || result != nil {
			t.Fatal("corrupt archive returned importable records")
		}
	}
	records := sample(t)
	if _, err := Encode(context.Background(), &bytes.Buffer{}, append(records, records[0])); err == nil {
		t.Fatal("duplicate ID archived")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Verify(ctx, bytes.NewReader(body.Bytes()), &descriptor); err == nil {
		t.Fatal("cancellation ignored")
	}
}

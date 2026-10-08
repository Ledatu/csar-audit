// Package archive implements the verified logical audit archive format.
// A verified chunk is not proof of sealed-partition coverage or permission to retire rows.
package archive

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/ledatu/csar-core/audit"
)

const (
	SchemaVersion      = 1
	MaxChunkBytes      = 32 << 20
	MaxCompressedBytes = 33 << 20
	MaxChunkRows       = 100_000
)

// Record keeps the canonical event intact. Historical receipt times may be unknown.
type Record struct {
	Event      audit.Event `json:"event"`
	ReceivedAt *time.Time  `json:"received_at,omitempty"`
}

// Chunk is a content descriptor. ETags never substitute for either SHA256.
type Chunk struct {
	SchemaVersion      int        `json:"schema_version"`
	Rows               int        `json:"rows"`
	CompressedBytes    int64      `json:"compressed_bytes"`
	UncompressedBytes  int64      `json:"uncompressed_bytes"`
	CompressedSHA256   string     `json:"compressed_sha256"`
	UncompressedSHA256 string     `json:"uncompressed_sha256"`
	EventIDsSHA256     string     `json:"event_ids_sha256"`
	OccurredMin        time.Time  `json:"occurred_min"`
	OccurredMax        time.Time  `json:"occurred_max"`
	ReceivedMin        *time.Time `json:"received_min,omitempty"`
	ReceivedMax        *time.Time `json:"received_max,omitempty"`
	UnknownReceiptRows int        `json:"unknown_receipt_rows"`
}

type countingWriter struct {
	writer io.Writer
	n      int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	w.n += int64(n)
	return n, err
}

// Encode emits deterministic gzip NDJSON and returns a descriptor only on completion.
func Encode(ctx context.Context, w io.Writer, records []Record) (Chunk, error) {
	if len(records) == 0 || len(records) > MaxChunkRows {
		return Chunk{}, errors.New("archive row count outside bounds")
	}
	rawHash := sha256.New()
	compressedHash := sha256.New()
	counted := &countingWriter{writer: io.MultiWriter(w, compressedHash)}
	gz, err := gzip.NewWriterLevel(counted, gzip.BestSpeed)
	if err != nil {
		return Chunk{}, err
	}
	// Closing partial output never makes it a valid archive receipt.
	defer func() { _ = gz.Close() }()
	descriptor, err := describe(records)
	if err != nil {
		return Chunk{}, err
	}
	for i := range records {
		if err := ctx.Err(); err != nil {
			return Chunk{}, err
		}
		line, err := json.Marshal(&records[i])
		if err != nil {
			return Chunk{}, err
		}
		line = append(line, '\n')
		descriptor.UncompressedBytes += int64(len(line))
		if descriptor.UncompressedBytes > MaxChunkBytes {
			return Chunk{}, errors.New("archive chunk exceeds uncompressed byte limit")
		}
		if _, err := io.MultiWriter(gz, rawHash).Write(line); err != nil {
			return Chunk{}, err
		}
	}
	if err := gz.Close(); err != nil {
		return Chunk{}, err
	}
	if counted.n > MaxCompressedBytes {
		return Chunk{}, errors.New("archive compressed byte limit exceeded")
	}
	descriptor.CompressedBytes = counted.n
	descriptor.CompressedSHA256 = hex.EncodeToString(compressedHash.Sum(nil))
	descriptor.UncompressedSHA256 = hex.EncodeToString(rawHash.Sum(nil))
	return descriptor, nil
}

// Verify validates the complete chunk before returning importable records.
// Bounded buffering prevents callers from importing a prefix of a corrupt object.
func Verify(ctx context.Context, r io.Reader, expected *Chunk) ([]Record, error) {
	if expected == nil || expected.SchemaVersion != SchemaVersion || expected.Rows < 1 || expected.Rows > MaxChunkRows ||
		expected.UncompressedBytes < 1 || expected.UncompressedBytes > MaxChunkBytes ||
		expected.CompressedBytes < 1 || expected.CompressedBytes > MaxCompressedBytes {
		return nil, errors.New("invalid archive descriptor bounds/version")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	compressed, err := io.ReadAll(io.LimitReader(r, expected.CompressedBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(compressed)) != expected.CompressedBytes || digest(compressed) != expected.CompressedSHA256 {
		return nil, errors.New("archive compressed checksum/size mismatch")
	}
	gz, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer func() { _ = gz.Close() }()
	raw, err := io.ReadAll(io.LimitReader(gz, expected.UncompressedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("archive gzip: %w", err)
	}
	if int64(len(raw)) != expected.UncompressedBytes || digest(raw) != expected.UncompressedSHA256 {
		return nil, errors.New("archive uncompressed checksum/size mismatch")
	}
	if raw[len(raw)-1] != '\n' {
		return nil, errors.New("archive lacks final NDJSON newline")
	}
	lines := bytes.Split(raw[:len(raw)-1], []byte{'\n'})
	if len(lines) != expected.Rows {
		return nil, errors.New("archive row count mismatch")
	}
	records := make([]Record, 0, len(lines))
	for _, line := range lines {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var record Record
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return nil, err
		}
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			return nil, errors.New("archive row has trailing data")
		}
		records = append(records, record)
	}
	actual, err := describe(records)
	if err != nil {
		return nil, err
	}
	actual.CompressedBytes = expected.CompressedBytes
	actual.UncompressedBytes = expected.UncompressedBytes
	actual.CompressedSHA256 = expected.CompressedSHA256
	actual.UncompressedSHA256 = expected.UncompressedSHA256
	a, err := json.Marshal(actual)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(expected)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(a, b) {
		return nil, errors.New("archive event coverage/time bounds mismatch")
	}
	return records, nil
}

func describe(records []Record) (Chunk, error) {
	descriptor := Chunk{SchemaVersion: SchemaVersion, Rows: len(records)}
	ids := make([]string, 0, len(records))
	seen := make(map[string]struct{}, len(records))
	for i := range records {
		record := &records[i]
		if len(record.Event.ID) != 36 || strings.ToLower(record.Event.ID) != record.Event.ID || record.Event.CreatedAt.IsZero() {
			return Chunk{}, errors.New("archive requires canonical ID and timestamp")
		}
		if err := audit.ValidateEventID(record.Event.ID); err != nil {
			return Chunk{}, err
		}
		if _, ok := seen[record.Event.ID]; ok {
			return Chunk{}, errors.New("archive contains duplicate ID")
		}
		seen[record.Event.ID] = struct{}{}
		ids = append(ids, record.Event.ID)
		ts := record.Event.CreatedAt
		if descriptor.OccurredMin.IsZero() || ts.Before(descriptor.OccurredMin) {
			descriptor.OccurredMin = ts
		}
		if descriptor.OccurredMax.IsZero() || ts.After(descriptor.OccurredMax) {
			descriptor.OccurredMax = ts
		}
		if record.ReceivedAt == nil {
			descriptor.UnknownReceiptRows++
			continue
		}
		received := *record.ReceivedAt
		if received.IsZero() {
			return Chunk{}, errors.New("zero receipt timestamp")
		}
		if descriptor.ReceivedMin == nil || received.Before(*descriptor.ReceivedMin) {
			descriptor.ReceivedMin = &received
		}
		if descriptor.ReceivedMax == nil || received.After(*descriptor.ReceivedMax) {
			descriptor.ReceivedMax = &received
		}
	}
	slices.Sort(ids)
	descriptor.EventIDsSHA256 = digest([]byte(strings.Join(ids, "\n") + "\n"))
	return descriptor, nil
}

func digest(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }

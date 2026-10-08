package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/ledatu/csar-core/storage"
)

const MaxManifestBytes int64 = 64 << 10

// Restore verifies a pinned manifest and complete pinned data before returning
// any records. Callers decide import/authorization; this does not mutate a DB.
func Restore(ctx context.Context, objects Objects, manifestKey, version string) (*Manifest, []Record, error) {
	if err := storage.ValidateObjectKey(manifestKey); err != nil {
		return nil, nil, err
	}
	if !strings.HasPrefix(manifestKey, "audit/v1/") || !strings.HasSuffix(manifestKey, "/manifest.json") || !pinnedVersion(version) {
		return nil, nil, errors.New("invalid archive manifest reference")
	}
	reader, err := objects.Open(ctx, manifestKey, version, MaxManifestBytes)
	if err != nil {
		return nil, nil, err
	}
	body, err := io.ReadAll(io.LimitReader(reader, MaxManifestBytes+1))
	closeErr := reader.Close()
	if err != nil {
		return nil, nil, err
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	if int64(len(body)) > MaxManifestBytes {
		return nil, nil, errors.New("archive manifest oversized")
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, nil, errors.New("archive manifest trailing data")
	}
	prefix, err := batchPrefix(manifest.Environment, manifest.BatchID, manifest.PlannedAt)
	if err != nil {
		return nil, nil, err
	}
	if manifest.SchemaVersion != SchemaVersion || manifestKey != prefix+"manifest.json" || manifest.DataKey != prefix+"events-0001.ndjson.gz" || !pinnedVersion(manifest.DataVersionID) {
		return nil, nil, errors.New("archive manifest reference/version mismatch")
	}
	reader, err = objects.Open(ctx, manifest.DataKey, manifest.DataVersionID, manifest.Chunk.CompressedBytes)
	if err != nil {
		return nil, nil, err
	}
	records, err := Verify(ctx, reader, &manifest.Chunk)
	closeErr = reader.Close()
	if err != nil {
		return nil, nil, err
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	return &manifest, records, nil
}

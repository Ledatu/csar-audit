package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/ledatu/csar-core/storage"
)

// ObjectReceipt pins a version; content integrity and retention are separate.
type ObjectReceipt struct {
	VersionID string `json:"version_id"`
}

type Objects interface {
	Put(context.Context, string, io.ReadSeeker, int64, string) (ObjectReceipt, error)
	Open(context.Context, string, string, int64) (io.ReadCloser, error)
}

type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	BatchID       string    `json:"batch_id"`
	Environment   string    `json:"environment"`
	PlannedAt     time.Time `json:"planned_at"`
	DataKey       string    `json:"data_key"`
	DataVersionID string    `json:"data_version_id"`
	Chunk         Chunk     `json:"chunk"`
}

type ExportReceipt struct {
	ManifestKey       string
	ManifestVersionID string
	Manifest          Manifest
}

// Export writes data, verifies the pinned version, then writes/verifies a manifest.
// The caller must fence the catalog commit separately; this never retires history.
func Export(ctx context.Context, objects Objects, environment, batchID string, plannedAt time.Time, records []Record) (*ExportReceipt, error) {
	prefix, err := batchPrefix(environment, batchID, plannedAt)
	if err != nil {
		return nil, err
	}
	dataKey := prefix + "events-0001.ndjson.gz"
	var compressed bytes.Buffer
	descriptor, err := Encode(ctx, &compressed, records)
	if err != nil {
		return nil, err
	}
	dataReceipt, err := objects.Put(ctx, dataKey, bytes.NewReader(compressed.Bytes()), int64(compressed.Len()), "application/gzip")

	if err != nil {
		return nil, err
	}
	if !pinnedVersion(dataReceipt.VersionID) {
		return nil, errors.New("archive upload lacks pinned version receipt")
	}
	reader, err := objects.Open(ctx, dataKey, dataReceipt.VersionID, descriptor.CompressedBytes)
	if err != nil {
		return nil, err
	}
	_, verifyErr := Verify(ctx, reader, &descriptor)
	closeErr := reader.Close()
	if verifyErr != nil {
		return nil, verifyErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	manifest := Manifest{
		SchemaVersion: SchemaVersion, BatchID: batchID, Environment: environment, PlannedAt: plannedAt.UTC(),
		DataKey: dataKey, DataVersionID: dataReceipt.VersionID, Chunk: descriptor,
	}
	encoded, err := json.Marshal(&manifest)
	if err != nil {
		return nil, err
	}
	manifestKey := prefix + "manifest.json"
	receipt, err := objects.Put(ctx, manifestKey, bytes.NewReader(encoded), int64(len(encoded)), "application/json")
	if err != nil {
		return nil, err
	}
	if !pinnedVersion(receipt.VersionID) {
		return nil, errors.New("archive manifest lacks pinned version receipt")
	}
	reader, err = objects.Open(ctx, manifestKey, receipt.VersionID, int64(len(encoded)))
	if err != nil {
		return nil, err
	}
	downloaded, readErr := io.ReadAll(io.LimitReader(reader, int64(len(encoded))+1))
	closeErr = reader.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if !bytes.Equal(downloaded, encoded) {
		return nil, errors.New("archive manifest verification failed")
	}
	return &ExportReceipt{ManifestKey: manifestKey, ManifestVersionID: receipt.VersionID, Manifest: manifest}, nil
}

func pinnedVersion(version string) bool { return version != "" && version != "null" }

func batchPrefix(environment, batchID string, plannedAt time.Time) (string, error) {
	id, err := uuid.Parse(batchID)
	if err != nil || id == uuid.Nil || id.String() != batchID {
		return "", errors.New("archive batch ID must be canonical nonzero UUID")
	}
	if strings.TrimSpace(environment) != environment {
		return "", errors.New("archive environment must be canonical")
	}
	if err := storage.ValidateScopeName(environment); err != nil {
		return "", err
	}
	if plannedAt.IsZero() {
		return "", errors.New("archive planning timestamp required")
	}
	prefix := fmt.Sprintf("audit/v1/%s/%s/%s/", environment, plannedAt.UTC().Format("2006/01/02/hour-15"), batchID)
	if err := storage.ValidateObjectKey(prefix + "manifest.json"); err != nil {
		return "", err
	}
	return prefix, nil
}

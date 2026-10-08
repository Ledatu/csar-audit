package archive

import (
	"context"
	"errors"
	"net/url"
	"path"
	"reflect"
	"strings"

	"github.com/ledatu/csar-core/storage"
)

// ReplicaLocation identifies an object namespace, not an independent failure domain.
type ReplicaLocation struct {
	Endpoint, Bucket, Prefix, Environment string
}

// ValidateReplicaLocations prevents copying back into the same object namespace.
// Credentials and region differences do not make a namespace distinct.
func ValidateReplicaLocations(source, destination ReplicaLocation) error {
	if source.Environment != destination.Environment || strings.TrimSpace(source.Environment) != source.Environment {
		return errors.New("replica environments must match")
	}
	if err := storage.ValidateScopeName(source.Environment); err != nil {
		return errors.New("replica environment invalid")
	}
	identity := func(location ReplicaLocation) (string, error) {
		endpoint, err := url.Parse(location.Endpoint)
		if err != nil || endpoint.Hostname() == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.RawPath != "" {
			return "", errors.New("replica endpoint invalid")
		}
		if location.Bucket == "" || strings.TrimSpace(location.Bucket) != location.Bucket {
			return "", errors.New("replica bucket invalid")
		}
		prefix, err := storage.JoinObjectKey(location.Prefix, "audit/v1/check")
		if err != nil {
			return "", errors.New("replica prefix invalid")
		}
		port := endpoint.Port()
		if (endpoint.Scheme == "http" && port == "80") || (endpoint.Scheme == "https" && port == "443") {
			port = ""
		}
		// Treat HTTP/HTTPS default endpoints as the same namespace: changing TLS
		// does not make a new copy. DNS aliases still require an operator review.
		return strings.TrimRight(strings.ToLower(endpoint.Hostname()), ".") + ":" + port + strings.TrimRight(path.Clean("/"+endpoint.Path), "/") + "|" + strings.ToLower(location.Bucket) + "|" + prefix, nil
	}
	from, err := identity(source)
	if err != nil {
		return err
	}
	to, err := identity(destination)
	if err != nil {
		return err
	}
	if from == to {
		return errors.New("replica destination must be a distinct object namespace")
	}
	return nil
}

// Replicate verifies the source before any destination write and emits a newly
// verified destination receipt. Partial uploads remain available for retry.
func Replicate(ctx context.Context, source, destination Objects, from, to ReplicaLocation, receipt *ExportReceipt) (*ExportReceipt, error) {
	if err := ValidateReplicaLocations(from, to); err != nil {
		return nil, err
	}
	if source == nil || destination == nil || receipt == nil || receipt.Manifest.Environment != from.Environment {
		return nil, errors.New("replica source receipt or objects invalid")
	}
	manifest, records, err := Restore(ctx, source, receipt.ManifestKey, receipt.ManifestVersionID)
	if err != nil {
		return nil, errors.New("replica source verification failed; destination untouched")
	}
	if !reflect.DeepEqual(*manifest, receipt.Manifest) {
		return nil, errors.New("replica source descriptor mismatch; destination untouched")
	}
	copied, err := Export(ctx, destination, manifest.Environment, manifest.BatchID, manifest.PlannedAt, records)
	if err != nil {
		return nil, errors.New("replica export failed; any destination objects retained")
	}
	verified, restored, err := Restore(ctx, destination, copied.ManifestKey, copied.ManifestVersionID)
	if err != nil || !reflect.DeepEqual(*verified, copied.Manifest) || !reflect.DeepEqual(restored, records) {
		return nil, errors.New("replica destination verification failed; objects retained")
	}
	return copied, nil
}

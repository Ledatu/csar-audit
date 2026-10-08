package archive

import (
	"context"
	"errors"
	"io"

	"github.com/ledatu/csar-core/s3store"
)

// S3Objects adapts the shared client; no separate credentials or S3 implementation.
type S3Objects struct{ Client *s3store.Client }

func (o S3Objects) Put(ctx context.Context, key string, body io.ReadSeeker, size int64, contentType string) (ObjectReceipt, error) {
	ref, err := o.Client.PutStreamIfAbsent(ctx, key, body, size, contentType)
	if errors.Is(err, s3store.ErrObjectExists) {
		ref, err = o.Client.StatStreamObject(ctx, key)
		if err == nil && ref.Size != size {
			return ObjectReceipt{}, errors.New("existing archive object length differs")
		}
	}
	// Export verifies existing bytes against its freshly computed descriptor.
	return ObjectReceipt{VersionID: ref.VersionID}, err
}
func (o S3Objects) Open(ctx context.Context, key, version string, maxBytes int64) (io.ReadCloser, error) {
	return o.Client.OpenVersion(ctx, key, version, maxBytes)
}

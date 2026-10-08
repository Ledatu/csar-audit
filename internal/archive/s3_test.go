package archive

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ledatu/csar-core/s3store"
	"github.com/ledatu/csar-core/secret"
	"github.com/ledatu/csar-core/ycloud"
)

// A successful upload with a missing receipt must be recoverable through HEAD
// and pinned verification, without producing another version on each retry.
func TestS3ExportRecoversUncertainManifestReceipt(t *testing.T) {
	for _, mode := range []string{"static", "iam_token"} {
		t.Run(mode, func(t *testing.T) {
			var mu sync.Mutex
			objects := make(map[string][]byte)
			writes := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if !strings.HasPrefix(r.URL.Path, "/test-bucket/prefix/audit/v1/prod/") {
					t.Error("archive escaped its destination", r.URL.Path)
				}
				body, exists := objects[r.URL.Path]
				switch r.Method {
				case http.MethodPut:
					if r.Header.Get("If-None-Match") != "*" {
						t.Error("archive overwrite was permitted")
					}
					if exists {
						w.WriteHeader(http.StatusPreconditionFailed)
						_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
						return
					}
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					objects[r.URL.Path] = body
					writes++
					// The first manifest upload persists, but loses its version receipt.
					if !strings.HasSuffix(r.URL.Path, "manifest.json") {
						w.Header().Set("x-amz-version-id", "original-version")
					}
				case http.MethodHead, http.MethodGet:
					if !exists {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					w.Header().Set("x-amz-version-id", "original-version")
					w.Header().Set("Content-Length", strconv.Itoa(len(body)))
					if r.Method == http.MethodGet {
						if r.URL.Query().Get("versionId") != "original-version" {
							t.Error("read did not pin the original version")
						}
						_, _ = w.Write(body)
					}
				default:
					t.Error("unexpected S3 method", r.Method)
				}
			}))
			defer server.Close()
			client, err := s3store.NewClient(&s3store.Config{Bucket: "test-bucket", Endpoint: server.URL, Region: "test", Prefix: "prefix", Auth: ycloud.AuthConfig{AuthMode: mode, AccessKeyID: secret.NewSecret("local-test-key"), SecretAccessKey: secret.NewSecret("local-test-secret"), IAMToken: secret.NewSecret("local-test-token")}}, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = client.Close() }()
			adapter := S3Objects{Client: client}
			ctx := context.Background()
			batch, planned, records := uuid.NewString(), time.Now().UTC(), sample(t)
			if receipt, err := Export(ctx, adapter, "prod", batch, planned, records); err == nil || receipt != nil {
				t.Fatal("uncertain upload advanced the catalog")
			}
			receipt, err := Export(ctx, adapter, "prod", batch, planned, records)
			if err != nil {
				t.Fatal(err)
			}
			_, restored, err := Restore(ctx, adapter, receipt.ManifestKey, receipt.ManifestVersionID)
			if err != nil || len(restored) != 1 || restored[0].Event.ID != records[0].Event.ID {
				t.Fatal("recovered receipt could not restore the same event", err)
			}
			mu.Lock()
			count := writes
			mu.Unlock()
			if count != 2 {
				t.Fatal("retry created additional object versions", count)
			}
		})
	}
}

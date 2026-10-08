package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeObjects struct {
	versions                                  map[string][]byte
	puts                                      []string
	opens                                     []string
	corruptData, corruptManifest, omitVersion bool
	failPut                                   int
}

func (o *fakeObjects) Put(_ context.Context, key string, r io.ReadSeeker, _ int64, _ string) (ObjectReceipt, error) {
	o.puts = append(o.puts, key)
	if o.failPut == len(o.puts) {
		return ObjectReceipt{}, errors.New("S3 unavailable")
	}
	body, err := io.ReadAll(r)
	if err != nil {
		return ObjectReceipt{}, err
	}
	version := fmt.Sprintf("v%d", len(o.puts))
	if o.versions == nil {
		o.versions = make(map[string][]byte)
	}
	o.versions[key+"@"+version] = body
	if o.omitVersion {
		return ObjectReceipt{}, nil
	}
	return ObjectReceipt{VersionID: version}, nil
}
func (o *fakeObjects) Open(_ context.Context, key, version string, _ int64) (io.ReadCloser, error) {
	o.opens = append(o.opens, key+"@"+version)
	body, ok := o.versions[key+"@"+version]
	if !ok {
		return nil, errors.New("pinned version missing")
	}
	body = bytes.Clone(body)
	if o.corruptData && strings.HasSuffix(key, ".gz") || o.corruptManifest && strings.HasSuffix(key, ".json") {
		body[0] ^= 1
	}
	return io.NopCloser(bytes.NewReader(body)), nil
}

func TestDataVerifiedBeforeManifest(t *testing.T) {
	objects := &fakeObjects{}
	receipt, err := Export(context.Background(), objects, "prod", uuid.NewString(), time.Now().UTC(), sample(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(objects.puts) != 2 || len(objects.opens) != 2 || receipt.ManifestVersionID != "v2" || receipt.Manifest.DataVersionID != "v1" {
		t.Fatal("missing pinned verification receipts")
	}
	if strings.Contains(objects.puts[0], "user") {
		t.Fatal("raw personal identifier in object key")
	}
}

func TestExportFailureNeverReturnsCatalogReceipt(t *testing.T) {
	for _, test := range []struct {
		name    string
		objects fakeObjects
		puts    int
	}{
		{"data corruption", fakeObjects{corruptData: true}, 1},
		{"missing version", fakeObjects{omitVersion: true}, 1},
		{"manifest corruption", fakeObjects{corruptManifest: true}, 2},
		{"upload unavailable", fakeObjects{failPut: 1}, 1},
		{"manifest upload unavailable", fakeObjects{failPut: 2}, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			receipt, err := Export(context.Background(), &test.objects, "prod", uuid.NewString(), time.Now().UTC(), sample(t))
			if err == nil || receipt != nil || len(test.objects.puts) != test.puts {
				t.Fatal("unverified history became cataloguable")
			}
		})
	}
}

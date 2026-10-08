package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type replicaObjects struct {
	objects     map[string][]byte
	versions    map[string]string
	puts        int
	reads       int
	label       string
	loseReceipt int
	corrupt     bool
}

func (o *replicaObjects) Put(_ context.Context, key string, body io.ReadSeeker, _ int64, _ string) (ObjectReceipt, error) {
	o.puts++
	if o.objects == nil {
		o.objects = map[string][]byte{}
		o.versions = map[string]string{}
	}
	if _, ok := o.objects[key]; !ok {
		data, err := io.ReadAll(body)
		if err != nil {
			return ObjectReceipt{}, err
		}
		o.objects[key] = data
		o.versions[key] = fmt.Sprintf("%s-v%d", o.label, len(o.objects))
	}
	if o.puts == o.loseReceipt {
		return ObjectReceipt{}, errors.New("provider failed secret=do-not-print")
	}
	return ObjectReceipt{VersionID: o.versions[key]}, nil
}
func (o *replicaObjects) Open(_ context.Context, key, version string, _ int64) (io.ReadCloser, error) {
	o.reads++
	if o.versions[key] != version {
		return nil, errors.New("version missing secret=do-not-print")
	}
	data := bytes.Clone(o.objects[key])
	if o.corrupt && strings.HasSuffix(key, ".gz") {
		data[0] ^= 1
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func replicaLocations() (ReplicaLocation, ReplicaLocation) {
	from := ReplicaLocation{Endpoint: "https://source.example", Bucket: "audit", Environment: "prod"}
	to := ReplicaLocation{Endpoint: "https://destination.example", Bucket: "audit", Environment: "prod"}
	return from, to
}
func TestReplicatePreservesLogicalDataAndDestinationVersions(t *testing.T) {
	source := &replicaObjects{label: "source"}
	destination := &replicaObjects{label: "destination"}
	receipt, err := Export(context.Background(), source, "prod", uuid.NewString(), time.Now().UTC(), sample(t))
	if err != nil {
		t.Fatal(err)
	}
	original := map[string][]byte{}
	for k, v := range source.objects {
		original[k] = bytes.Clone(v)
	}
	from, to := replicaLocations()
	copied, err := Replicate(context.Background(), source, destination, from, to, receipt)
	if err != nil {
		t.Fatal(err)
	}
	if copied.Manifest.DataVersionID == receipt.Manifest.DataVersionID || copied.ManifestVersionID == receipt.ManifestVersionID {
		t.Fatal("destination references source versions")
	}
	if copied.Manifest.BatchID != receipt.Manifest.BatchID || !copied.Manifest.PlannedAt.Equal(receipt.Manifest.PlannedAt) || !reflect.DeepEqual(copied.Manifest.Chunk, receipt.Manifest.Chunk) {
		t.Fatal("logical coverage changed")
	}
	retried, err := Replicate(context.Background(), source, destination, from, to, receipt)
	if err != nil || !reflect.DeepEqual(copied, retried) {
		t.Fatal("retry changed destination receipt", err)
	}
	if len(destination.objects) != 2 || !reflect.DeepEqual(source.objects, original) || source.puts != 2 {
		t.Fatal("source mutated or retry added versions")
	}
}
func TestReplicateLostUploadReceiptRecovers(t *testing.T) {
	for _, lost := range []int{1, 2} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			source := &replicaObjects{label: "source"}
			destination := &replicaObjects{label: "destination", loseReceipt: lost}
			receipt, err := Export(context.Background(), source, "prod", uuid.NewString(), time.Now().UTC(), sample(t))
			if err != nil {
				t.Fatal(err)
			}
			from, to := replicaLocations()
			if got, err := Replicate(context.Background(), source, destination, from, to, receipt); err == nil || got != nil || strings.Contains(err.Error(), "secret") {
				t.Fatal("uncertain export became verified or leaked error", err)
			}
			versions := map[string]string{}
			for k, v := range destination.versions {
				versions[k] = v
			}
			got, err := Replicate(context.Background(), source, destination, from, to, receipt)
			if err != nil || got == nil {
				t.Fatal(err)
			}
			for k, v := range versions {
				if destination.versions[k] != v {
					t.Fatal("uncertain object overwritten")
				}
			}
			if len(destination.objects) != 2 {
				t.Fatal("retry incomplete")
			}
		})
	}
}
func TestReplicateRejectsSourceBeforeDestinationWrites(t *testing.T) {
	for _, problem := range []string{"corrupt", "descriptor", "environment", "same namespace"} {
		t.Run(problem, func(t *testing.T) {
			source := &replicaObjects{label: "source"}
			destination := &replicaObjects{label: "destination"}
			receipt, err := Export(context.Background(), source, "prod", uuid.NewString(), time.Now().UTC(), sample(t))
			if err != nil {
				t.Fatal(err)
			}
			from, to := replicaLocations()
			switch problem {
			case "corrupt":
				source.corrupt = true
			case "descriptor":
				receipt.Manifest.Chunk.Rows++
			case "environment":
				to.Environment = "dev"
			case "same namespace":
				to = from
				to.Endpoint = "http://SOURCE.example:80/"
				to.Prefix = "/"
			}
			got, err := Replicate(context.Background(), source, destination, from, to, receipt)
			if err == nil || got != nil || destination.puts != 0 {
				t.Fatal("unverified source caused destination writes")
			}
		})
	}
}
func TestReplicaLocationGuards(t *testing.T) {
	from, to := replicaLocations()
	for _, endpoint := range []string{"https://user:secret@example", "https://example?token=secret", "https://example#fragment", "file:///tmp/objects", "not-a-url"} {
		bad := to
		bad.Endpoint = endpoint
		if err := ValidateReplicaLocations(from, bad); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("bad endpoint accepted or leaked")
		}
	}
	for _, prefix := range []string{"../elsewhere", "a/../elsewhere"} {
		bad := to
		bad.Prefix = prefix
		if err := ValidateReplicaLocations(from, bad); err == nil {
			t.Fatal("unsafe prefix accepted")
		}
	}
}

func TestReplicaNamespaceCanonicalization(t *testing.T) {
	from, _ := replicaLocations()
	for _, endpoint := range []string{"https://SOURCE.example:443/", "http://source.example:80/", "https://source.example././"} {
		to := from
		to.Endpoint = endpoint
		to.Prefix = "/"
		if err := ValidateReplicaLocations(from, to); err == nil {
			t.Fatal("equivalent endpoint accepted", endpoint)
		}
	}
	to := from
	to.Prefix = "optional-copy"
	if err := ValidateReplicaLocations(from, to); err != nil {
		t.Fatal("distinct prefix rejected", err)
	}
}

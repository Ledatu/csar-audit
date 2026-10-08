package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type replicaEndpoint struct {
	mu              sync.Mutex
	bodies          map[string][]byte
	versions        map[string]string
	destinationPuts int
	corruptSource   bool
}

func (e *replicaEndpoint) serve(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	key := r.URL.Path
	if r.Method == http.MethodPut {
		if strings.HasPrefix(key, "/destination/") {
			e.destinationPuts++
		}
		if r.Header.Get("If-None-Match") != "*" {
			t.Error("nonconditional replica write")
		}
		if _, ok := e.bodies[key]; ok {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		e.bodies[key] = body
		e.versions[key] = fmt.Sprintf("version-%d", len(e.bodies))
		w.Header().Set("x-amz-version-id", e.versions[key])
		return
	}
	body, ok := e.bodies[key]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("x-amz-version-id", e.versions[key])
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		return
	}
	if r.Method != http.MethodGet || r.URL.Query().Get("versionId") != e.versions[key] {
		t.Error("replica read did not pin version")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body = bytes.Clone(body)
	if e.corruptSource && strings.HasPrefix(key, "/source/") && strings.HasSuffix(key, ".gz") {
		body[0] ^= 1
	}
	_, _ = w.Write(body)
}
func replicaConfigFile(t *testing.T, dir, name, endpoint, environment string) string {
	t.Helper()
	file := filepath.Join(dir, name+".yaml")
	body := fmt.Sprintf("archive:\n  environment: %s\n  bucket: %s\n  endpoint: %s\n  region: test\n  auth:\n    auth_mode: static\n    access_key_id: dummy\n    secret_access_key: do-not-print-secret\n", environment, name, endpoint)
	if err := os.WriteFile(file, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}
func TestReplicaCLIEndToEndAndGuards(t *testing.T) {
	objects := &replicaEndpoint{bodies: map[string][]byte{}, versions: map[string]string{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { objects.serve(t, w, r) }))
	defer server.Close()
	dir := t.TempDir()
	source := replicaConfigFile(t, dir, "source", server.URL, "preflight")
	destination := replicaConfigFile(t, dir, "destination", server.URL, "preflight")
	receiptFile := filepath.Join(dir, "source.json")
	if err := run(context.Background(), []string{"probe", "--config", source, "--receipt-file", receiptFile}, io.Discard); err != nil {
		t.Fatal(err)
	}
	original := map[string][]byte{}
	for k, v := range objects.bodies {
		original[k] = bytes.Clone(v)
	}
	args := func(output string) []string {
		return []string{"replicate", "--source-config", source, "--destination-config", destination, "--receipt-file", receiptFile, "--output-receipt", output}
	}
	outputFile := filepath.Join(dir, "copy.json")
	var output bytes.Buffer
	if err := run(context.Background(), args(outputFile), &output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "do-not-print-secret") || strings.Contains(output.String(), "system:archive-preflight") {
		t.Fatal("replica report exposes credentials or event payload")
	}
	stat, err := os.Stat(outputFile)
	if err != nil || stat.Mode().Perm() != 0o600 {
		t.Fatal("receipt mode not protected", err)
	}
	from, err := loadReceipt(receiptFile)
	if err != nil {
		t.Fatal(err)
	}
	to, err := loadReceipt(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	if from.ManifestVersionID == to.ManifestVersionID || from.Manifest.DataVersionID == to.Manifest.DataVersionID {
		t.Fatal("destination reused source versions")
	}
	retryFile := filepath.Join(dir, "retry.json")
	if err := run(context.Background(), args(retryFile), io.Discard); err != nil {
		t.Fatal(err)
	}
	retry, err := loadReceipt(retryFile)
	if err != nil || !reflect.DeepEqual(to, retry) {
		t.Fatal("retry changed receipt", err)
	}
	before := objects.destinationPuts
	if err := run(context.Background(), args(outputFile), io.Discard); err == nil || objects.destinationPuts != before {
		t.Fatal("existing receipt overwritten or wrote destination")
	}
	if err := run(context.Background(), []string{"verify", "--config", destination, "--receipt-file", outputFile}, io.Discard); err != nil {
		t.Fatal("copied archive not recoverable", err)
	}
	objects.mu.Lock()
	objects.corruptSource = true
	objects.mu.Unlock()
	if err := run(context.Background(), args(filepath.Join(dir, "corrupt.json")), io.Discard); err == nil || objects.destinationPuts != before {
		t.Fatal("corrupt source written")
	}
	for k, v := range original {
		if !bytes.Equal(v, objects.bodies[k]) {
			t.Fatal("source archive overwritten")
		}
	}
	same := args(filepath.Join(dir, "same.json"))
	same[4] = source
	if err := run(context.Background(), same, io.Discard); err == nil || objects.destinationPuts != before {
		t.Fatal("same namespace accepted")
	}
	differentEnv := replicaConfigFile(t, dir, "different", server.URL, "prod")
	mismatch := args(filepath.Join(dir, "mismatch.json"))
	mismatch[4] = differentEnv
	if err := run(context.Background(), mismatch, io.Discard); err == nil || objects.destinationPuts != before {
		t.Fatal("different environments accepted")
	}
	badReceipt := filepath.Join(dir, "wrong-receipt.json")
	if err := os.WriteFile(badReceipt, []byte(`{"Manifest":{"environment":"different"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	badArgs := args(filepath.Join(dir, "wrong-receipt-output.json"))
	badArgs[6] = badReceipt
	if err := run(context.Background(), badArgs, io.Discard); err == nil || objects.destinationPuts != before {
		t.Fatal("mismatched receipt accepted")
	}
	if err := os.WriteFile(destination, []byte("database:\n  dsn: do-not-print-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), args(filepath.Join(dir, "bad-config.json")), io.Discard); err == nil || strings.Contains(err.Error(), "do-not-print-secret") || objects.destinationPuts != before {
		t.Fatal("runtime config accepted or exposed")
	}
}
func TestReplicaCLIArgumentsNeverDiscloseValues(t *testing.T) {
	for _, args := range [][]string{{"replicate"}, {"replicate", "--unknown", "do-not-print-secret"}, {"replicate", "--source-config", "do-not-print-secret", "extra"}, {"replicate", "--source-config", "do-not-print-secret", "--destination-config", "missing", "--receipt-file", "missing", "--output-receipt", "missing"}} {
		var out bytes.Buffer
		err := run(context.Background(), args, &out)
		if err == nil || strings.Contains(err.Error(), "do-not-print-secret") || out.Len() != 0 {
			t.Fatal("invalid arguments accepted or disclosed values")
		}
	}
}

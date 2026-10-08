package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestRestoreTargetGuards(t *testing.T) {
	for _, dsn := range []string{"", "postgres://test:test@production/csar_audit_restore?sslmode=disable", "postgres://test:test@127.0.0.1/csar_audit?sslmode=disable", "postgres://test:test@127.0.0.1,production/csar_audit_restore?sslmode=disable", "postgres://test:test@127.0.0.1/csar_audit_restore?sslmode=prefer"} {
		if _, err := restoreConfig(dsn); err == nil {
			t.Fatal("unsafe target accepted")
		}
	}
	cfg, err := restoreConfig("postgres://test:test@localhost:55432/csar_audit_restore?sslmode=disable&options=-csearch_path%3Dother")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Host != "127.0.0.1" || cfg.ConnConfig.RuntimeParams["search_path"] != "public" || cfg.ConnConfig.RuntimeParams["options"] != "" {
		t.Fatal("DSN options or DNS can redirect import")
	}
	// Refuse a bad target before even opening config or fetching S3 bytes.
	t.Setenv("AUDIT_RESTORE_DATABASE_URL", "postgres://test:test@production/csar_audit_restore?sslmode=disable")
	if err := run(context.Background(), []string{"restore-local", "--config", "missing", "--receipt-file", "missing"}, io.Discard); err == nil || !strings.Contains(err.Error(), "localhost") {
		t.Fatal("target guard ran too late", err)
	}
}

func TestCLIProbeAndPinnedVerification(t *testing.T) {
	var mu sync.Mutex
	objects := map[string][]byte{}
	corrupt := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Method == http.MethodPut {
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("probe did not use conditional write")
			}
			if _, exists := objects[r.URL.Path]; exists {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			objects[r.URL.Path] = body
			w.Header().Set("x-amz-version-id", "pinned-version")
			return
		}
		if r.Method == http.MethodHead {
			w.Header().Set("x-amz-version-id", "pinned-version")
			w.Header().Set("Content-Length", strconv.Itoa(len(objects[r.URL.Path])))
			return
		}
		if r.Method != http.MethodGet || r.URL.Query().Get("versionId") != "pinned-version" {
			t.Error("verification was not a pinned GET")
		}
		body := bytes.Clone(objects[r.URL.Path])
		if corrupt && strings.HasSuffix(r.URL.Path, ".gz") {
			body[0] ^= 1
		}
		w.Header().Set("x-amz-version-id", "pinned-version")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	dir := t.TempDir()
	cfg := filepath.Join(dir, "reader.yaml")
	receipt := filepath.Join(dir, "receipt.json")
	text := fmt.Sprintf("archive:\n  environment: preflight\n  bucket: test-bucket\n  endpoint: %s\n  region: test\n  auth:\n    auth_mode: static\n    access_key_id: dummy\n    secret_access_key: dummy-secret\n", server.URL)
	if err := os.WriteFile(cfg, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run(context.Background(), []string{"probe", "--config", cfg, "--receipt-file", receipt}, &output); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Rows     int  `json:"rows"`
		Verified bool `json:"verified"`
	}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil || report.Rows != 2 || !report.Verified {
		t.Fatal("probe report invalid", err)
	}
	if strings.Contains(output.String(), "system:archive-preflight") || strings.Contains(output.String(), "dummy-secret") {
		t.Fatal("report exposed event payload or credentials")
	}
	if err := run(context.Background(), []string{"probe", "--config", cfg, "--receipt-file", receipt}, io.Discard); err == nil {
		t.Fatal("probe receipt overwritten")
	}
	if err := run(context.Background(), []string{"verify", "--config", cfg, "--receipt-file", receipt}, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"probe-replay", "--config", cfg, "--receipt-file", receipt}, io.Discard); err != nil {
		t.Fatal("conditional replay failed", err)
	}
	mu.Lock()
	corrupt = true
	mu.Unlock()
	if err := run(context.Background(), []string{"verify", "--config", cfg, "--receipt-file", receipt}, io.Discard); err == nil {
		t.Fatal("corrupt archive verified")
	}
	if err := os.WriteFile(cfg, []byte(text+"database:\n  dsn: not-for-this-tool\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadArchiveConfig(cfg); err == nil {
		t.Fatal("runtime config silently accepted")
	}
}

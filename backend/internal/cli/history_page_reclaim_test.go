//go:build linux

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func TestHistoryPageReclaimClientTypedUnixAndLimits(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "api.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan map[string]any, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/history/reclaim" {
			t.Error("unexpected request")
		}
		var request map[string]any
		json.NewDecoder(r.Body).Decode(&request)
		seen <- request
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"unsupported","reason":"auto_vacuum_not_incremental","commit_state":"not_started","transaction_committed":false}`))
	})}
	go server.Serve(listener)
	defer server.Shutdown(context.Background())
	cmd := newHistoryPageReclaimCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs([]string{"--socket", socket, "--database-device", "1", "--database-inode", "2", "--max-pages", "3", "--min-free-pages", "5", "--write-budget", "20ms"})
	if err = cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	request := <-seen
	if request["max_pages"] != float64(3) || request["min_free_pages"] != float64(5) || request["write_budget_ns"] != float64(20000000) || len(request) != 5 {
		t.Fatal(request)
	}
	var result map[string]any
	if err = json.Unmarshal(output.Bytes(), &result); err != nil || result["status"] != "unsupported" {
		t.Fatal(output.String(), err)
	}
}

func TestHistoryPageReclaimClientRejectsUnsafeUsage(t *testing.T) {
	for _, args := range [][]string{{}, {"--socket", "/unused", "--database-device", "1", "--database-inode", "2", "--max-pages", "-1"}, {"--socket", "/unused", "--database-device", "1", "--database-inode", "2", "--timeout", "0s"}} {
		cmd := newHistoryPageReclaimCommand()
		cmd.SetArgs(args)
		if err := cmd.Execute(); err == nil {
			t.Fatal("unsafe usage accepted")
		}
	}
}

func TestHistoryPageReclaimClientDoesNotPrintUnknownNestedContent(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "api.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok","before":{"body":"NEVER-PRINT-SYNTHETIC"}}`))
	})}
	go server.Serve(listener)
	defer server.Shutdown(context.Background())
	cmd := newHistoryPageReclaimCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--socket", socket, "--database-device", "1", "--database-inode", "2"})
	if err = cmd.Execute(); err == nil || strings.Contains(out.String(), "NEVER-PRINT") {
		t.Fatal("untyped content exposed", err)
	}
}

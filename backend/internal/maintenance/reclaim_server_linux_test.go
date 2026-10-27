//go:build linux

package maintenance

import (
	"context"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func TestUnixPrivatePageReclaimSmokeAndStrictRequest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ao.db")
	setup, err := sql.Open("sqlite", path+"?_pragma=auto_vacuum(2)")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(`CREATE TABLE planted_payload(id INTEGER PRIMARY KEY,body BLOB)`); err != nil {
		t.Fatal(err)
	}
	tx, err := setup.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		if _, err = tx.Exec(`INSERT INTO planted_payload VALUES(?,zeroblob(8192))`, i); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = setup.Exec(`DELETE FROM planted_payload WHERE id<255`); err != nil {
		t.Fatal(err)
	}
	setup.Close()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	parent := filepath.Join(dir, "private")
	if err = os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(parent, "api.sock")
	reclaim := adapter.DefaultPageReclaimOptions()
	reclaim.MinFreePages = 1
	reclaim.MaxPages = 3
	server, err := StartWithPageReclaim(socket, s, adapter.DefaultHistoryMaintenanceOptions(), &reclaim)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	response, err := client.Get("http://localhost/v1/capability")
	if err != nil {
		t.Fatal(err)
	}
	var cap map[string]any
	err = json.NewDecoder(response.Body).Decode(&cap)
	response.Body.Close()
	page, ok := cap["pageReclaim"].(map[string]any)
	if err != nil || !ok || page["supported"] != true || page["maxPages"] != float64(3) {
		t.Fatal(cap, err)
	}
	req := adapter.PageReclaimRequest{DatabaseDevice: uint64(cap["dbDevice"].(float64)), DatabaseInode: uint64(cap["dbInode"].(float64)), MaxPages: 2}
	body, _ := json.Marshal(req)
	posted, err := client.Post("http://localhost/v1/history/reclaim", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	var result adapter.PageReclaimResult
	err = json.NewDecoder(posted.Body).Decode(&result)
	posted.Body.Close()
	if err != nil || posted.StatusCode != 200 || !result.Committed || result.FreelistPagesRemoved != 2 || result.AttemptedSteps != 2 || result.Before.AutoVacuum != 2 {
		t.Fatal(result, err)
	}
	for _, body := range []string{`{"sql":"VACUUM"}`, `{"path":"/other.db"}`, `{"max_pages":1} {"max_pages":99}`} {
		res, e := client.Post("http://localhost/v1/history/reclaim", "application/json", strings.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		res.Body.Close()
		if res.StatusCode != 400 {
			t.Fatal("untyped request accepted", res.StatusCode)
		}
	}
	forwarded, _ := http.NewRequest("POST", "http://localhost/v1/history/reclaim", strings.NewReader(string(body)))
	forwarded.Header.Set("Origin", "https://foreign.invalid")
	denied, err := client.Do(forwarded)
	if err != nil {
		t.Fatal(err)
	}
	denied.Body.Close()
	if denied.StatusCode != 403 {
		t.Fatal("forwarded reclaim accepted")
	}
}

func TestInvalidNewReclaimConfigKeepsExistingHistorySocket(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	parent := filepath.Join(dir, "private")
	os.Mkdir(parent, 0700)
	socket := filepath.Join(parent, "api.sock")
	t.Setenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET", socket)
	t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_MAX_PAGES", "bad")
	server, err := StartConfigured(s)
	if err != nil || server == nil {
		t.Fatal("new invalid setting disabled existing maintenance", err)
	}
	defer server.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	res, err := client.Get("http://localhost/v1/capability")
	if err != nil {
		t.Fatal(err)
	}
	var cap map[string]any
	json.NewDecoder(res.Body).Decode(&cap)
	res.Body.Close()
	page := cap["pageReclaim"].(map[string]any)
	if cap["schemaReady"] != true || page["configured"] != false {
		t.Fatal(cap)
	}
	res, err = client.Post("http://localhost/v1/history/reclaim", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	var result adapter.PageReclaimResult
	json.NewDecoder(res.Body).Decode(&result)
	res.Body.Close()
	if res.StatusCode != 200 || result.Status != "unsupported" || result.Committed {
		t.Fatal(result)
	}
}

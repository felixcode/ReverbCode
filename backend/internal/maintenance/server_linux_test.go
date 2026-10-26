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

func TestUnixPrivateMaintenance(t *testing.T) {
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	parent := filepath.Join(dir, "private")
	if err = os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(parent, "api.sock")
	server, err := Start(path, s, adapter.DefaultHistoryMaintenanceOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatal("private socket permissions", err)
	}
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	response, err := client.Get("http://localhost/v1/capability")
	if err != nil {
		t.Fatal(err)
	}
	var cap map[string]any
	err = json.NewDecoder(response.Body).Decode(&cap)
	response.Body.Close()
	if err != nil || cap["maintenanceProtocol"] != float64(1) || cap["schemaReady"] != true {
		t.Fatal(cap, err)
	}
	// End-to-end typed Unix request performs a genuine eligible write, not
	// merely a zero-change scan/capability probe.
	db, e := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec(`INSERT INTO telemetry_event(id,occurred_at,name,source,level,payload_json)VALUES('old','2020-01-01T00:00:00Z','synthetic','synthetic','info','{}')`); e != nil {
		t.Fatal(e)
	}
	identity := s.HistoryRetentionCapability()
	requestBody, e := json.Marshal(adapter.HistoryMaintenanceRequest{Stage: "builtin:telemetry_event", Cutoff: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano), DatabaseDevice: identity["dbDevice"].(uint64), DatabaseInode: identity["dbInode"].(uint64), BatchRows: 1})
	if e != nil {
		t.Fatal(e)
	}
	posted, e := client.Post("http://localhost/v1/history/batch", "application/json", strings.NewReader(string(requestBody)))
	if e != nil {
		t.Fatal(e)
	}
	var changed map[string]any
	e = json.NewDecoder(posted.Body).Decode(&changed)
	posted.Body.Close()
	if e != nil || posted.StatusCode != 200 || changed["deleted"] != float64(1) || changed["transaction_committed"] != true || changed["commit_state"] != "committed" {
		t.Fatal("typed write smoke", changed, e)
	}
	var remaining int
	if e = db.QueryRow(`SELECT count(*) FROM telemetry_event`).Scan(&remaining); e != nil || remaining != 0 {
		t.Fatal("eligible row not deleted", e)
	}
	for _, header := range []string{"Origin", "Forwarded", "X-Forwarded-For"} {
		req, _ := http.NewRequest("GET", "http://localhost/v1/capability", nil)
		req.Header.Set(header, "blocked")
		res, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		res.Body.Close()
		if res.StatusCode != 403 {
			t.Fatal("forwarded access permitted", header, res.StatusCode)
		}
	}
	req, _ := http.NewRequest("POST", "http://localhost/v1/history/batch", strings.NewReader(`{"stage":"raw","sql":"DELETE FROM sessions"}`))
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 400 {
		t.Fatal("arbitrary operation accepted", res.StatusCode)
	}
	// Existing paths/foreign files are not overwritten or unlinked.
	if _, err = Start(path, s, adapter.DefaultHistoryMaintenanceOptions()); err == nil {
		t.Fatal("existing listener overwritten")
	}
	if err = os.Chmod(parent, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err = Start(filepath.Join(parent, "other.sock"), s, adapter.DefaultHistoryMaintenanceOptions()); err == nil {
		t.Fatal("public parent accepted")
	}
}

func TestMaintenanceInvalidSettingsDoNotStartListener(t *testing.T) {
	for _, value := range []string{"NaN", "+Inf", "0", "-1", "1e100", "0.0000000000000000000001"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET", filepath.Join(t.TempDir(), "api.sock"))
			t.Setenv("ALBANIA_AO_HISTORY_TRANSACTION_SECONDS", value)
			server, err := StartConfigured(nil)
			if err == nil || server != nil {
				if server != nil {
					server.Close()
				}
				t.Fatal("invalid settings accepted")
			}
		})
	}
}

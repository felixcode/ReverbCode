//go:build linux

package maintenance

import (
	"context"
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

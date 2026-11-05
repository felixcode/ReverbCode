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
	"testing"
	"time"

	adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
	_ "modernc.org/sqlite"
)

// Use only the capability's small metadata tables, not application migrations.
func configuredDurationCapability(t *testing.T, statement, transaction string) (*Server, map[string]any) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "fixture.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec(`CREATE TABLE goose_db_version(version_id INTEGER,is_applied INTEGER);INSERT INTO goose_db_version VALUES(191,1)`); err != nil {
		t.Fatal(err)
	}
	if err = adapter.EnsureHistoryRetentionSchema(db); err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{
		"MAINTENANCE_SOCKET": filepath.Join(dir, "api.sock"),
		"RETENTION_HOURS":    "12", "RAW_CACHE_RETENTION_HOURS": "6", "CLOSED_OWNER_RETENTION_HOURS": "24",
		"STATEMENT_SECONDS": statement, "TRANSACTION_SECONDS": transaction,
	} {
		t.Setenv("ALBANIA_AO_HISTORY_"+key, value)
	}
	server, err := StartConfigured(adapter.NewStore(db, db))
	if err != nil || server == nil {
		t.Fatal("configured listener", err)
	}
	t.Cleanup(func() { _ = server.Close() })
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", filepath.Join(dir, "api.sock"))
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Timeout: time.Second, Transport: transport}
	response, err := client.Get("http://localhost/v1/capability")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var cap map[string]any
	if err = json.NewDecoder(response.Body).Decode(&cap); err != nil || response.StatusCode != http.StatusOK || cap["schemaReady"] != true {
		t.Fatal("capability", cap, err)
	}
	return server, cap
}

func TestStartConfiguredClosedOwnerHoursReachUnixCapability(t *testing.T) {
	_, cap := configuredDurationCapability(t, "0.75", "1.25")
	if cap["closedOwnerRetentionHours"] != float64(24) || cap["retentionHours"] != float64(12) || cap["rawCacheRetentionHours"] != float64(6) {
		t.Fatal("hour settings changed units", cap)
	}
}

func TestStartConfiguredSecondBudgetsRemainSeconds(t *testing.T) {
	server, cap := configuredDurationCapability(t, "0.75", "1.25")
	if cap["writeBudgetMS"] != float64(1250) || server.server.ReadTimeout != 750*time.Millisecond || server.server.ReadHeaderTimeout != 750*time.Millisecond || server.server.WriteTimeout != 2*time.Second {
		t.Fatal("second budgets changed units", cap)
	}
}

func TestStartConfiguredInvalidClosedOwnerHoursRefuseListener(t *testing.T) {
	t.Setenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET", filepath.Join(t.TempDir(), "api.sock"))
	t.Setenv("ALBANIA_AO_HISTORY_CLOSED_OWNER_RETENTION_HOURS", "100000000")
	server, err := StartConfigured(nil)
	if err == nil || server != nil {
		if server != nil {
			_ = server.Close()
		}
		t.Fatal("overflow hours accepted")
	}
}

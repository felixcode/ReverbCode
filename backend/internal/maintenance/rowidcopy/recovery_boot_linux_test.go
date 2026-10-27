//go:build linux

package rowidcopy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
	aosqlite "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func sealRecoveryFixture(t *testing.T, dir string) string {
	t.Helper()
	if stat, err := os.Stat(filepath.Join(dir, "ao.db-wal")); err == nil && stat.Size() != 0 || err != nil && !os.IsNotExist(err) {
		t.Fatal("fixture_not_standalone_after_close")
	}
	source := filepath.Join(dir, "sealed.sqlite3")
	if err := os.Rename(filepath.Join(dir, "ao.db"), source); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(source, 0600); err != nil {
		t.Fatal(err)
	}
	return source
}

func rebuildRecoveryFixture(t *testing.T, source string) string {
	t.Helper()
	o := opts(t, source)
	r, err := Run(context.Background(), o)
	if err != nil || r.Status != "complete_unpublished" || r.AutoVacuum != 2 {
		t.Fatal("fixture_rebuild_incomplete", err)
	}
	// Only this newly created synthetic candidate is renamed. It is never a
	// production install operation, nor an adoption of an existing work volume.
	if err := os.Rename(filepath.Join(o.Directory, "candidate.sqlite3"), filepath.Join(o.Directory, "ao.db")); err != nil {
		t.Fatal(err)
	}
	return o.Directory
}

func TestRebuiltApplicationStoreKeepsOriginalQueuedInputAndNewIdentity(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := aosqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	now := time.Now().UTC()
	if err = s.UpsertProject(ctx, domain.ProjectRecord{ID: "recovery", Path: t.TempDir(), RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	rec, err := s.CreateSession(ctx, domain.SessionRecord{ID: "recovery-1", ProjectID: "recovery", Harness: domain.HarnessCodex, Mode: domain.SessionModeChat, Kind: domain.KindWorker, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.CreateConversation(ctx, "recovery-conv", domain.ConversationScopeSession, rec.ProjectID, rec.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ClaimChatControllerGeneration(ctx, rec.ID, "original-generation"); err != nil {
		t.Fatal(err)
	}
	msg := domain.ConversationMessage{ID: "original-input", ClientMessageID: "original-client-id", Text: "synthetic queued input", Origin: domain.MessageOriginHuman}
	if created, err := s.AppendUserMessage(ctx, c.ID, rec.ID, "original-generation", msg, "original-turn", now); err != nil || !created {
		t.Fatal("fixture_input_not_created", err)
	}
	before, err := s.LoadConversationSnapshot(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	identity := s.HistoryRetentionCapability()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	newDir := rebuildRecoveryFixture(t, sealRecoveryFixture(t, dir))
	reopened, err := aosqlite.OpenPreMigrated(newDir)
	if err != nil {
		t.Fatal("rebuilt_application_open_failed", err)
	}
	defer reopened.Close()
	freshIdentity := reopened.HistoryRetentionCapability()
	if identity["dbDevice"] == freshIdentity["dbDevice"] && identity["dbInode"] == freshIdentity["dbInode"] {
		t.Fatal("new_candidate_reused_old_identity")
	}
	after, err := reopened.LoadConversationSnapshot(ctx, c.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rebuilt_projection_or_input_changed", err)
	}
	retry := msg
	retry.ID = "must-not-become-a-new-input"
	if created, err := reopened.AppendUserMessage(ctx, c.ID, rec.ID, "original-generation", retry, "must-not-become-a-new-turn", now); err != nil || created {
		t.Fatal("original_client_id_was_redispatched", err)
	}
	retry.Text = "changed synthetic input"
	if created, err := reopened.AppendUserMessage(ctx, c.ID, rec.ID, "original-generation", retry, "must-not-become-a-new-turn", now); created || !errors.Is(err, domain.ErrClientMessageConflict) {
		t.Fatal("changed_payload_was_accepted", err)
	}
	final, err := reopened.LoadConversationSnapshot(ctx, c.ID)
	if err != nil || !reflect.DeepEqual(before, final) {
		t.Fatal("input_retry_changed_original_projection", err)
	}
	t.Log("rebuilt_store_open=true new_db_identity=true original_input_id=true original_client_id=true duplicate_new_rows=0 provider_calls=0")
}

// The externally built binary runs only against an EMPTY rebuilt fixture. No
// provider host, session, credential, or original AO data directory is supplied.
func TestRebuiltEmptyApplicationDaemonBootSmoke(t *testing.T) {
	binary := os.Getenv("AO_RECOVERY_SMOKE_BINARY")
	if binary == "" {
		t.Skip("isolated_runner_supplies_built_binary")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("smoke_binary_must_be_absolute")
	}
	dir := t.TempDir()
	s, err := aosqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	data := rebuildRecoveryFixture(t, sealRecoveryFixture(t, dir))
	bootRecoveryData(t, binary, data, true)
}

func bootRecoveryData(t *testing.T, binary, data string, empty bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	home := t.TempDir()
	runtimeDir := t.TempDir()
	if err = os.Chmod(runtimeDir, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "daemon")
	cmd.Env = []string{"PATH=/usr/local/go/bin:/usr/bin:/bin", "HOME=" + home, "XDG_RUNTIME_DIR=" + runtimeDir,
		"AO_DATA_DIR=" + data, "AO_RUN_FILE=" + filepath.Join(data, "running.json"), "AO_PORT=" + strconv.Itoa(port),
		"AO_REQUEST_TIMEOUT=5m", "AO_TELEMETRY_EVENTS=off", "AO_TELEMETRY_METRICS=off", "AO_TELEMETRY_REMOTE=off",
		"AO_ACP_NOTIFICATION_QUEUE_CAPACITY=65536", "AO_ACP_NOTIFICATION_QUEUE_BYTES=134217728"}
	cmd.Dir = home
	// No persisted daemon logs are needed for this empty-fixture health check.
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err = cmd.Start(); err != nil {
		t.Fatal("fixture_daemon_start_failed", err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("fixture_child_not_reaped")
		}
	}()
	client := &http.Client{Timeout: 250 * time.Millisecond, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(20 * time.Second)
	ready := false
	for time.Now().Before(deadline) && !ready {
		select {
		case <-done:
			t.Fatal("fixture_daemon_exited_before_health")
		default:
		}
		record, e := runfile.Read(filepath.Join(data, "running.json"))
		if e == nil && record != nil && record.PID == cmd.Process.Pid && record.Port == port {
			ready = true
			for _, endpoint := range []string{"healthz", "readyz"} {
				response, e := client.Get(fmt.Sprintf("http://127.0.0.1:%d/%s", port, endpoint))
				if e != nil {
					ready = false
					break
				}
				var probe struct {
					PID int `json:"pid"`
				}
				e = json.NewDecoder(io.LimitReader(response.Body, 16384)).Decode(&probe)
				response.Body.Close()
				if e != nil || response.StatusCode != 200 || probe.PID != cmd.Process.Pid {
					ready = false
					break
				}
			}
		}
		if !ready {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !ready {
		t.Fatal("fixture_daemon_health_not_ready")
	}
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal("fixture_daemon_stop_failed", err)
	}
	select {
	case <-done:
		if waitErr != nil {
			t.Fatal("fixture_daemon_not_graceful", waitErr)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("fixture_daemon_stop_timeout")
	}
	if !empty {
		t.Log("real_daemon_health=true real_daemon_ready=true graceful_stop=true fixture_empty=false native_replay=not_exercised")
		return
	}
	closed, err := aosqlite.OpenReadOnly(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	defer closed.Close()
	sessions, err := closed.ListAllSessions(context.Background())
	if err != nil || empty && len(sessions) != 0 {
		t.Fatal("empty_boot_created_sessions", err)
	}
	t.Logf("real_daemon_health=true real_daemon_ready=true graceful_stop=true fixture_empty=%t private_session_rows=%d native_replay=not_exercised", empty, len(sessions))
}

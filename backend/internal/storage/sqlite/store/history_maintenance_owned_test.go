package store_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	adapter "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func TestOwnedMaintenanceTerminalAndBusyProtection(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedProject(t, s, "owned")
	rec := sampleRecord("owned")
	rec.Mode = domain.SessionModeChat
	session, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := s.CreateConversation(ctx, "owned-conv", domain.ConversationScopeSession, "owned", session.ID, histClock)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ClaimChatControllerGeneration(ctx, session.ID, "gen"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"old", "busy"} {
		_, err = s.AppendUserMessage(ctx, conv.ID, session.ID, "gen", domain.ConversationMessage{ID: id, Text: "UNIQUE-EXPIRED-SENTINEL", Origin: domain.MessageOriginHuman, ClientMessageID: id}, id+"-turn", histClock)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.BindTurnToProvider(ctx, id+"-turn", id+"-native", histClock); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SettleTurnByID(ctx, "old-turn", domain.TurnStateCompleted, "", histClock); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, id := range []string{"old", "busy"} {
		if _, err = db.Exec(`INSERT INTO conversation_provider_events(conversation_id,session_id,provider_event_id,method,payload_json,received_at)VALUES(?,?,?,?,?,?)`, conv.ID, session.ID, id, "message.completed", `{"providerTurnId":"`+id+`-native","text":"UNIQUE-EXPIRED-SENTINEL","error":null}`, histClock); err != nil {
			t.Fatal(err)
		}
	}
	cap := s.HistoryRetentionCapability()
	options := adapter.DefaultHistoryMaintenanceOptions()
	options.WriteBudget = time.Second
	req := adapter.HistoryMaintenanceRequest{Stage: "message", Cutoff: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano), DatabaseDevice: cap["dbDevice"].(uint64), DatabaseInode: cap["dbInode"].(uint64)}
	result, err := s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || !result.Committed || result.Compacted != 1 {
		t.Fatalf("batch: %+v %v", result, err)
	}
	var old, busy string
	if err = db.QueryRow(`SELECT text FROM conversation_messages WHERE id='old'`).Scan(&old); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT text FROM conversation_messages WHERE id='busy'`).Scan(&busy); err != nil {
		t.Fatal(err)
	}
	if old != "[AO history expired]" || busy != "UNIQUE-EXPIRED-SENTINEL" {
		t.Fatal("active content changed")
	}
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 0 || result.Markers != 0 {
		t.Fatal("idempotence", result, err)
	}
	req.Stage = "raw"
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 1 {
		t.Fatal("raw", result, err)
	}
	var raw string
	db.QueryRow(`SELECT payload_json FROM conversation_provider_events WHERE provider_event_id='old'`).Scan(&raw)
	if strings.Contains(raw, "UNIQUE-EXPIRED-SENTINEL") {
		t.Fatal("raw body survived")
	}
	// A canceled maintenance request has no partial body/marker commit.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	req.Stage = "turn"
	result, err = s.MaintainHistoryBatch(canceled, req, options)
	if err == nil || result.Committed {
		t.Fatal("canceled request committed")
	}

	// All other payload stages use the same owned transaction and retain a
	// pending authorization, even when its associated turn is old and terminal.
	activity := domain.ConversationActivity{ID: "command", ProviderItemID: "command-native", Kind: domain.ActivityKindCommand, Status: domain.ActivityStatusCompleted, Summary: "UNIQUE-EXPIRED-SENTINEL", Detail: []byte(`{"detail":"UNIQUE-EXPIRED-SENTINEL"}`)}
	if err = s.UpsertActivity(ctx, conv.ID, "old-native", activity, histClock); err != nil {
		t.Fatal(err)
	}
	pending := domain.ConversationActivity{ID: "pending", ProviderItemID: "pending-native", Kind: domain.ActivityKindApproval, Status: domain.ActivityStatusPending, Summary: "pending", Detail: []byte(`{}`), RequestID: "request"}
	if err = s.UpsertActivity(ctx, conv.ID, "old-native", pending, histClock); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE conversation_activities SET command_output='UNIQUE-EXPIRED-SENTINEL',streamed_text='UNIQUE-EXPIRED-SENTINEL' WHERE id='command'`); err != nil {
		t.Fatal(err)
	}
	req.Stage = "outputs"
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 0 {
		t.Fatal("pending authorization unprotected", result, err)
	}
	if _, err = db.Exec(`UPDATE conversation_activities SET status='resolved' WHERE id='pending'`); err != nil {
		t.Fatal(err)
	}
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 1 {
		t.Fatal("outputs", result, err)
	}
	req.Stage = "activity"
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 2 {
		t.Fatal("activity", result, err)
	}
	if _, err = db.Exec(`UPDATE conversation_turns SET diff_json='UNIQUE-EXPIRED-SENTINEL',plan_json='UNIQUE-EXPIRED-SENTINEL' WHERE id='old-turn'`); err != nil {
		t.Fatal(err)
	}
	req.Stage = "turn"
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 1 {
		t.Fatal("turn details", result, err)
	}
	if _, err = db.Exec(`UPDATE sessions SET prompt='UNIQUE-EXPIRED-SENTINEL',updated_at='2020-01-01T00:00:00Z' WHERE id=?`, session.ID); err != nil {
		t.Fatal(err)
	}
	req.Stage = "session"
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 0 {
		t.Fatal("active owner prompt", result, err)
	}
	if _, err = db.Exec(`UPDATE sessions SET is_terminated=1 WHERE id=?`, session.ID); err != nil {
		t.Fatal(err)
	}
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 0 {
		t.Fatal("queued owner prompt", result, err)
	}
	if _, err = db.Exec(`UPDATE conversation_turns SET state='completed',completed_at='2020-01-01T00:00:00Z' WHERE id='busy-turn'`); err != nil {
		t.Fatal(err)
	}
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Compacted != 1 {
		t.Fatal("closed owner prompt", result, err)
	}
	req.DatabaseInode++
	if _, err = s.MaintainHistoryBatch(ctx, req, options); err == nil {
		t.Fatal("changed DB identity accepted")
	}
}

func TestOwnedMaintenanceYieldsToProjectionAndRollsBackOnTimeout(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedProject(t, s, "contention")
	rec := sampleRecord("contention")
	rec.Mode = domain.SessionModeChat
	session, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := s.CreateConversation(ctx, "contention-conv", domain.ConversationScopeSession, "contention", session.ID, histClock)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ClaimChatControllerGeneration(ctx, session.ID, "gen"); err != nil {
		t.Fatal(err)
	}
	cap := s.HistoryRetentionCapability()
	req := adapter.HistoryMaintenanceRequest{Stage: "message", Cutoff: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano), DatabaseDevice: cap["dbDevice"].(uint64), DatabaseInode: cap["dbInode"].(uint64)}
	// Ensure a PK candidate exists, so maintenance reaches writer admission.
	if _, err = s.AppendUserMessage(ctx, conv.ID, session.ID, "gen", domain.ConversationMessage{ID: "input", Text: "LIVE", Origin: domain.MessageOriginHuman}, "running", histClock); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, e := s.ProjectProviderEvent(ctx, conv.ID, session.ID, "gen", "new-live-event", "message.delta", `{"text":"LIVE"}`, time.Now(), func(context.Context) error { close(entered); <-release; return nil })
		done <- e
	}()
	<-entered
	options := adapter.DefaultHistoryMaintenanceOptions()
	began := time.Now()
	result, err := s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || result.Status != "deferred" || result.Committed || result.NextCursor != "" || time.Since(began) > time.Second {
		t.Fatalf("business admission: %+v %v", result, err)
	}
	close(release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	// An independent writer is not part of writeMu. A bounded context must still
	// roll back, preserve body/marker/cursor, and release the single AO writer.
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	external, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	if _, err = external.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	options.WriteBudget = 50 * time.Millisecond
	released := make(chan struct{})
	go func() { time.Sleep(500 * time.Millisecond); external.ExecContext(ctx, `ROLLBACK`); close(released) }()
	began = time.Now()
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	<-released
	if err == nil || result.Committed || result.Compacted != 0 || result.NextCursor != "" {
		t.Fatal("failed writer advanced receipt", result, err)
	}
	if result.WaitMS > 200 {
		t.Fatal("maintenance queued behind external writer")
	}
	options.WriteBudget = time.Second
	result, err = s.MaintainHistoryBatch(ctx, req, options)
	if err != nil || !result.Committed || result.Compacted != 0 {
		t.Fatal("writer reservation leaked", result, err)
	}
	applied, err := s.ProjectProviderEvent(ctx, conv.ID, session.ID, "gen", "new-live-event", "message.delta", `{}`, time.Now(), func(context.Context) error { t.Fatal("duplicate projection applied"); return nil })
	if err != nil || applied {
		t.Fatal("archive idempotence changed", err)
	}
}

func TestOwnedMaintenanceBuiltinBoundedDeletes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, v := range []struct{ id, stamp string }{{"old", "2020-01-01T00:00:00Z"}, {"fresh", time.Now().UTC().Format(time.RFC3339Nano)}, {"unknown", "2026-01-01T00:00:00"}} {
		if _, err = db.Exec(`INSERT INTO telemetry_event(id,occurred_at,name,source,level,payload_json)VALUES(?,?,'synthetic','synthetic','info','{}')`, v.id, v.stamp); err != nil {
			t.Fatal(err)
		}
	}
	cap := s.HistoryRetentionCapability()
	o := adapter.DefaultHistoryMaintenanceOptions()
	o.BatchRows = 2
	o.WriteBudget = time.Second
	req := adapter.HistoryMaintenanceRequest{Stage: "builtin:telemetry_event", Cutoff: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano), DatabaseDevice: cap["dbDevice"].(uint64), DatabaseInode: cap["dbInode"].(uint64)}
	r, err := s.MaintainHistoryBatch(ctx, req, o)
	if err != nil || r.Scanned != 2 || r.Deleted != 1 || r.Complete {
		t.Fatal("bounded builtin", r, err)
	}
	req.Cursor = r.NextCursor
	r, err = s.MaintainHistoryBatch(ctx, req, o)
	if err != nil || r.Retained["invalid_timestamp"] != 1 || r.Deleted != 0 {
		t.Fatal("unproven builtin age", r, err)
	}
	var n int
	if err = db.QueryRow(`SELECT count(*) FROM telemetry_event`).Scan(&n); err != nil || n != 2 {
		t.Fatal(n, err)
	}
}

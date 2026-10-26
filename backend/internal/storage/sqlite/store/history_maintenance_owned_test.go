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
	req.DatabaseInode++
	if _, err = s.MaintainHistoryBatch(ctx, req, options); err == nil {
		t.Fatal("changed DB identity accepted")
	}
}

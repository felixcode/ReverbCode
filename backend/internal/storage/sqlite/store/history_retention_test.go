package store_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func retentionHash(v string) string { h := sha256.Sum256([]byte(v)); return hex.EncodeToString(h[:]) }
func TestRetentionStoreKeepsStubsAndArchiveAfterLateEvents(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedProject(t, s, "retention")
	rec := sampleRecord("retention")
	rec.Mode = domain.SessionModeChat
	rec.Metadata.Prompt = "ORIGINAL-SENTINEL"
	session, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := s.CreateConversation(ctx, "retention-conv", domain.ConversationScopeSession, "retention", session.ID, histClock)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ClaimChatControllerGeneration(ctx, session.ID, "generation"); err != nil {
		t.Fatal(err)
	}
	msg := domain.ConversationMessage{ID: "user", Text: "old prompt", Origin: domain.MessageOriginHuman, ClientMessageID: "client"}
	if _, err = s.AppendUserMessage(ctx, conv.ID, session.ID, "generation", msg, "turn", histClock); err != nil {
		t.Fatal(err)
	}
	if err = s.BindTurnToProvider(ctx, "turn", "native", histClock); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleAssistantMessage(ctx, conv.ID, "answer", "native", "old body", "assistant", histClock); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleTurnByID(ctx, "turn", domain.TurnStateFailed, "old error", histClock); err != nil {
		t.Fatal(err)
	}
	activity := domain.ConversationActivity{ID: "activity", ProviderItemID: "tool", Kind: domain.ActivityKindCommand, Status: domain.ActivityStatusCompleted, Summary: "ORIGINAL-SENTINEL", Detail: []byte(`{"body":"ORIGINAL-SENTINEL"}`)}
	if err = s.UpsertActivity(ctx, conv.ID, "native", activity, histClock); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetTurnPlan(ctx, conv.ID, "native", domain.ConversationPlan{Explanation: "ORIGINAL-SENTINEL"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetTurnDiff(ctx, conv.ID, "native", domain.ConversationTurnDiff{Files: []domain.ConversationDiffFile{{Path: "ORIGINAL-SENTINEL"}}}, histClock); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"user", "assistant"} {
		if _, err = tx.Exec(`UPDATE conversation_messages SET text='[AO history expired]' WHERE id=?`, id); err != nil {
			t.Fatal(err)
		}
		semantic := retentionHash("assistant\x00old body")
		prompt := ""
		if id == "user" {
			semantic = retentionHash("user\x00old prompt")
			prompt = retentionHash("old prompt")
		}
		if _, err = tx.Exec(`INSERT INTO ao_history_retention VALUES('message',?,?,?, ?,8,?,'2026-10-25','2026-10-23','["text"]')`, id, conv.ID, semantic, prompt, retentionHash("dummy")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.Exec(`UPDATE conversation_turns SET error_message='[AO history expired]',diff_json='',plan_json='' WHERE id='turn'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO ao_history_retention VALUES('turn','turn',?,'','',8,?,'2026-10-25','2026-10-23','["error_message"]')`, conv.ID, retentionHash("dummy")); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE conversation_activities SET summary='[AO history expired]',detail_json='{"_ao_history_expired":true}',command_output='',streamed_text='' WHERE id='activity'`); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO ao_history_retention VALUES('activity','activity',?,?, '',8,?,'2026-10-25','2026-10-23','["summary"]')`, conv.ID, retentionHash("command\x00completed\x00ORIGINAL-SENTINEL\x00"+string(activity.Detail)), retentionHash("dummy")); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`UPDATE sessions SET prompt='[AO history expired]' WHERE id=?`, session.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO ao_history_retention VALUES('session',?,?,'','',8,?,'2026-10-25','2026-10-23','["prompt"]')`, session.ID, conv.ID, retentionHash("dummy")); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = s.AppendAssistantDelta(ctx, conv.ID, "answer", "native", "SHOULD-NOT-RETURN", "ignored", histClock); err != nil {
		t.Fatal(err)
	}
	if err = s.SettleAssistantMessage(ctx, conv.ID, "answer", "native", "RESURRECTED", "ignored", histClock); err != nil {
		t.Fatal(err)
	}
	activity.Summary = "RESURRECTED-SENTINEL"
	activity.Detail = []byte(`{"body":"RESURRECTED-SENTINEL"}`)
	if err = s.UpsertActivity(ctx, conv.ID, "native", activity, histClock); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AppendCommandOutput(ctx, conv.ID, "tool", "RESURRECTED-SENTINEL", histClock); err != nil {
		t.Fatal(err)
	}
	if _, err = s.AppendActivityStreamedText(ctx, conv.ID, "tool", "RESURRECTED-SENTINEL", histClock); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetTurnPlan(ctx, conv.ID, "native", domain.ConversationPlan{Explanation: "RESURRECTED-SENTINEL"}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.SetTurnDiff(ctx, conv.ID, "native", domain.ConversationTurnDiff{Files: []domain.ConversationDiffFile{{Path: "RESURRECTED-SENTINEL"}}}, histClock); err != nil {
		t.Fatal(err)
	}
	current, found, e := s.GetSession(ctx, session.ID)
	if e != nil || !found || !current.Metadata.PromptExpired {
		t.Fatalf("session expiry absent: %v", e)
	}
	current.Metadata.Prompt = "RESURRECTED-SENTINEL"
	if err = s.UpdateSession(ctx, current); err != nil {
		t.Fatal(err)
	}
	current, _, err = s.GetSession(ctx, session.ID)
	if err != nil || current.Metadata.Prompt != "[AO history expired]" {
		t.Fatal("session prompt rehydrated")
	}
	snapshot, err := s.LoadConversationSnapshot(ctx, conv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Activities) != 1 || !snapshot.Activities[0].ContentExpired || snapshot.Activities[0].Summary != "[AO history expired]" || snapshot.Activities[0].CommandOutput != "" || snapshot.Activities[0].StreamedText != "" {
		t.Fatal("activity body rehydrated")
	}
	if len(snapshot.Turns) != 1 || !snapshot.Turns[0].ContentExpired || snapshot.Turns[0].Diff != nil || snapshot.Turns[0].Plan != nil {
		t.Fatal("turn body rehydrated")
	}
	for _, m := range snapshot.Messages {
		if !m.ContentExpired || m.Text != "[AO history expired]" || m.RetentionSemanticSHA256 == "" {
			t.Fatalf("stub lost: %#v", m)
		}
	}
	if _, err = s.RetryPrompt(ctx, conv.ID, "turn"); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired retry allowed: %v", err)
	}
	called := false
	payload := `{"providerTurnId":"native","providerItemId":"changed-native-id","delta":"PRIVATE-OLD-BODY","text":"PRIVATE-OLD-BODY","summary":"PRIVATE-OLD-BODY","detail":{"body":"PRIVATE-OLD-BODY"},"diff":{"body":"PRIVATE-OLD-BODY"},"input":{"body":"PRIVATE-OLD-BODY"},"plan":{"body":"PRIVATE-OLD-BODY"},"error":null,"turnState":"completed","unknownFact":3}`
	if projected, eventErr := s.ProjectProviderEvent(ctx, conv.ID, session.ID, "generation", "late", "message/completed", payload, time.Now(), func(context.Context) error { called = true; return nil }); eventErr != nil || projected {
		t.Fatalf("expired late body projected: %v %v", projected, eventErr)
	}
	if called {
		t.Fatal("late expired projection ran")
	}
	if projected, e := s.ProjectProviderEvent(ctx, conv.ID, session.ID, "generation", "late-item", "message/delta", `{"providerItemId":"answer","delta":"PRIVATE-OLD-BODY","error":null}`, time.Now(), func(context.Context) error { called = true; return nil }); e != nil || projected {
		t.Fatalf("late item body projected: %v %v", projected, e)
	}
	events, err := s.ProviderEventsSince(ctx, conv.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || strings.Contains(events[0].PayloadJson+events[1].PayloadJson, "PRIVATE-OLD-BODY") {
		t.Fatalf("archive rehydrated prose: %#v", events)
	}
	var archived map[string]any
	if err = json.Unmarshal([]byte(events[0].PayloadJson), &archived); err != nil {
		t.Fatal(err)
	}
	if _, ok := archived["error"]; !ok || archived["error"] != nil || archived["unknownFact"] != float64(3) {
		t.Fatal("archive identity/error facts changed")
	}
	cap := s.HistoryRetentionCapability()
	if cap["schemaReady"] != true || cap["processStartTicks"] == nil {
		t.Fatalf("missing runtime identity capability: %#v", cap)
	}
}

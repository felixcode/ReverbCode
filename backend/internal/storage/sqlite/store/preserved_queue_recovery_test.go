package store_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func queueProofHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func preservedQueueFixture(t *testing.T, counts ...int) (*sqlite.Store, *sql.DB, domain.SessionID, domain.PreservedQueueRecoveryRequest) {
 count:=2
 if len(counts)>0 {count=counts[0]}
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	seedProject(t, s, "restore")
	rec := sampleRecord("restore")
	rec.Mode = domain.SessionModeChat
 rec.Harness = domain.HarnessClaudeCode
	session, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := s.CreateConversation(ctx, "queue-conversation", domain.ConversationScopeSession, "restore", session.ID, histClock)
	if err != nil {
		t.Fatal(err)
	}
	branch := domain.ConversationBranch{ID: "queue-branch", ConversationID: conv.ID, SessionID: session.ID, ProviderConversationID: "same-native", ParentBranchID: conv.ID + ":root", CreatedAt: histClock}
	if err = s.CreateAndActivateConversationBranch(ctx, session.ID, branch, "old-generation", histClock); err != nil {
		t.Fatal(err)
	}
	if err = s.AdoptProviderTurn(ctx, conv.ID, session.ID, "old-generation", "bound-original", "provider-original", histClock); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("queued-%d", i)
		text := fmt.Sprintf("original input %d", i)
		client := fmt.Sprintf("client-%d", i)
		payload := queueProofHash(text)
		if i < count*2/3 {
			client = ""
			payload = ""
		}
		if _, err = s.AppendUserMessage(ctx, conv.ID, session.ID, "old-generation", domain.ConversationMessage{ID: "message-" + id, Text: text, Role: domain.MessageRoleUser, Origin: domain.MessageOriginHuman, ClientMessageID: client, ClientPayloadHash: payload}, id, histClock.Add(time.Duration(i+1)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	failedAt := histClock.Add(time.Duration(count+10) * time.Second)
	if _, err = s.CleanupOwnedControllerWork(ctx, session.ID, conv.ID, "old-generation", failedAt); err != nil {
		t.Fatal(err)
	}
	if err = s.ClaimChatControllerGeneration(ctx, session.ID, "new-generation"); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err = db.Exec(`UPDATE conversation_turns SET state='running',completed_at=NULL,error_message='',controller_generation='new-generation' WHERE id='bound-original'`); err != nil {
		t.Fatal(err)
	}
	req := domain.PreservedQueueRecoveryRequest{Protocol: 1, ConversationID: conv.ID, BranchID: branch.ID, NativeID: "same-native", BoundTurnID: "bound-original", BoundProviderTurnID: "provider-original", BeforeSnapshotSHA256: queueProofHash("before snapshot")}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("queued-%d", i)
		var requested, failure, generation string
		if err = db.QueryRow(`SELECT CAST(requested_at AS TEXT),CAST(completed_at AS TEXT),controller_generation FROM conversation_turns WHERE id=?`, id).Scan(&requested, &failure, &generation); err != nil {
			t.Fatal(err)
		}
		req.FailureAt = failure
		var input domain.PreservedQueueInputProof
		var text, delivery string
		if err = db.QueryRow(`SELECT id,client_message_id,COALESCE(client_payload_hash,''),text,delivery_content_json FROM conversation_messages WHERE turn_id=?`, id).Scan(&input.ID, &input.ClientMessageID, &input.ClientPayloadHash, &text, &delivery); err != nil {
			t.Fatal(err)
		}
		input.TextSHA256 = queueProofHash(text)
		input.DeliverySHA256 = queueProofHash(delivery)
		req.Turns = append(req.Turns, domain.PreservedQueueTurnProof{ID: id, PreviousGeneration: generation, RequestedAt: requested, BeforeState: "queued", Inputs: []domain.PreservedQueueInputProof{input}})
	}
	return s, db, session.ID, req
}
func TestPreservedQueueRecoveryExactAtomicAndIdempotent(t *testing.T) {
	s, db, session, req := preservedQueueFixture(t)
	ctx := context.Background()
	result, err := s.RestorePreservedQueue(ctx, session, "new-generation", req, nil, histClock.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if result.Restored != 2 || result.Dispatch != "held_busy" {
		t.Fatalf("unexpected counts: %+v", result)
	}
	for _, proof := range req.Turns {
		var state, requested string
		var started, completed sql.NullString
		if err = db.QueryRow(`SELECT state,CAST(requested_at AS TEXT),started_at,completed_at FROM conversation_turns WHERE id=?`, proof.ID).Scan(&state, &requested, &started, &completed); err != nil {
			t.Fatal(err)
		}
		if state != "queued" || requested != proof.RequestedAt || started.Valid || completed.Valid {
			t.Fatal("original lifecycle/order changed")
		}
	}
	// Lost response retry after normal dispatch must never turn it back to queued.
	if err = s.BindTurnToProvider(ctx, req.Turns[0].ID, "now-dispatched", histClock.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	repeat, err := s.RestorePreservedQueue(ctx, session, "new-generation", req, nil, histClock.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if repeat.Restored != 0 || repeat.PreviouslyRestored != 2 {
		t.Fatal("non-idempotent retry")
	}
	var state string
	db.QueryRow(`SELECT state FROM conversation_turns WHERE id=?`, req.Turns[0].ID).Scan(&state)
	if state != "running" {
		t.Fatal("dispatched row reset")
	}
}
func TestPreservedQueueRecoveryRejectsChangedProofAsWholeBatch(t *testing.T) {
	for _, scenario := range []string{"text", "delivery", "generation", "requested", "failure", "client", "started", "cancelled", "retry", "branch", "owner", "bound-failed", "false-terminal", "rollback"} {
		t.Run(scenario, func(t *testing.T) {
			s, db, session, req := preservedQueueFixture(t)
			id := req.Turns[1].ID
			var statement string
			var args []any
			switch scenario {
			case "text":
				statement = `UPDATE conversation_messages SET text='changed' WHERE turn_id=?`
				args = []any{id}
			case "delivery":
				statement = `UPDATE conversation_messages SET delivery_content_json='[]' WHERE turn_id=?`
				args = []any{id}
			case "generation":
				req.Turns[1].PreviousGeneration = "other"
			case "requested":
				req.Turns[1].RequestedAt = "changed"
			case "failure":
				req.FailureAt = "other"
			case "client":
				req.Turns[0].Inputs[0].ClientMessageID = "mismatch"
			case "started":
				statement = `UPDATE conversation_turns SET started_at=CURRENT_TIMESTAMP WHERE id=?`
				args = []any{id}
			case "cancelled":
				statement = `UPDATE conversation_turns SET state='cancelled' WHERE id=?`
				args = []any{id}
			case "retry":
				statement = `UPDATE conversation_turns SET retry_of_turn_id=? WHERE id='bound-original'`
				args = []any{id}
			case "branch":
				req.BranchID = "other"
			case "owner":
				session = "other"
			case "bound-failed":
				statement = `UPDATE conversation_turns SET state='failed',error_message='provider error',completed_at=CURRENT_TIMESTAMP WHERE id='bound-original'`
			case "false-terminal":
				statement = `UPDATE conversation_turns SET state='completed',completed_at=CURRENT_TIMESTAMP WHERE id='bound-original'`
			case "rollback":
				statement = `UPDATE conversation_turns SET rolled_back_at=CURRENT_TIMESTAMP WHERE id=?`
				args = []any{id}
			}
			if statement != "" {
				if _, err := db.Exec(statement, args...); err != nil {
					t.Fatal(err)
				}
			}
			_, err := s.RestorePreservedQueue(context.Background(), session, "new-generation", req, nil, histClock.Add(time.Minute))
			if err == nil {
				t.Fatal("changed batch accepted")
			}
			var count int
			db.QueryRow(`SELECT count(*) FROM conversation_turns WHERE state='queued'`).Scan(&count)
			if count != 0 {
				t.Fatal("partial batch restored")
			}
		})
	}
}
func TestPreservedQueueRecoveryRequiresExactTerminalArchive(t *testing.T) {
	s, db, session, req := preservedQueueFixture(t)
	ctx := context.Background()
	at := histClock.Add(time.Minute)
	if _, err := s.ProjectProviderEvent(ctx, req.ConversationID, session, "new-generation", "completion-proof", "turn.completed", `{"providerTurnId":"provider-original","providerConversationId":"same-native","turnState":"completed","error":null}`, at, func(txCtx context.Context) error {
		return s.SettleTurn(txCtx, req.ConversationID, "provider-original", domain.TurnStateCompleted, "", at)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RestorePreservedQueue(ctx, session, "new-generation", req, []string{"foreign-proof"}, at); !errors.Is(err, domain.ErrPreservedQueueRecovery) {
		t.Fatalf("false evidence: %v", err)
	}
	result, err := s.RestorePreservedQueue(ctx, session, "new-generation", req, []string{"completion-proof"}, at)
	if err != nil {
		t.Fatal(err)
	}
	if result.Restored != 2 || result.Dispatch != "ready_after_commit" {
		t.Fatal("terminal restore not ready")
	}
	var count int
	db.QueryRow(`SELECT count(*) FROM conversation_turns`).Scan(&count)
	if count != 3 {
		t.Fatal("created a turn")
	}
}

func TestPreservedQueueRecoveryFull103WithLegacyClients(t *testing.T){
 s,db,session,req:=preservedQueueFixture(t,103)
 empty:=0;for _,turn:=range req.Turns{if turn.Inputs[0].ClientMessageID==""{empty++}}
 if len(req.Turns)!=103 || empty!=68{t.Fatal("fixture mixture drift")}
 result,err:=s.RestorePreservedQueue(context.Background(),session,"new-generation",req,nil,histClock.Add(time.Hour));if err!=nil{t.Fatal(err)}
 if result.Restored!=103{t.Fatal("not whole batch")}
 var turns,messages,queued int
 db.QueryRow(`SELECT count(*) FROM conversation_turns`).Scan(&turns)
 db.QueryRow(`SELECT count(*) FROM conversation_messages`).Scan(&messages)
 db.QueryRow(`SELECT count(*) FROM conversation_turns WHERE state='queued'`).Scan(&queued)
 if turns!=104 || messages!=103 || queued!=103{t.Fatal("IDs/inputs changed")}
}

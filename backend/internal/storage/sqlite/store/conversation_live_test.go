package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestLiveTurnReaderDoesNotReadTimelineOrTurnBodies(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedProject(t, s, "live")
	rec := sampleRecord("live")
	rec.Mode = domain.SessionModeChat
	rec.Metadata.ControllerGeneration = "generation-root"
	session, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	conv, err := s.CreateConversation(ctx, "live", domain.ConversationScopeSession, "live", session.ID, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendUserMessage(ctx, conv.ID, session.ID, "generation-root", domain.ConversationMessage{ID: "live-user", Origin: domain.MessageOriginHuman, Text: "private synthetic"}, "live-turn", testNow); err != nil {
		t.Fatal(err)
	}
	if err := s.BindTurnToProvider(ctx, "live-turn", "native-running", testNow); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`UPDATE conversation_turns SET plan_json='invalid-body',diff_json='invalid-body',error_message='unused body'`); err != nil {
		t.Fatal(err)
	}
	// A full timeline reader now errors while reading text/summary. Scalar
	// lineage EXISTS probes remain valid and must not evaluate those columns.
	for _, table := range []string{"conversation_messages", "conversation_activities"} {
		rows, err := db.Query(`SELECT name FROM pragma_table_info(?)`, table)
		if err != nil {
			t.Fatal(err)
		}
		var columns []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			column := fmt.Sprintf(`"%s"`, name)
			if name == "text" || name == "summary" || name == "detail_json" || name == "command_output" || name == "streamed_text" {
				column = `json_extract('invalid-json','$') AS ` + column
			}
			columns = append(columns, column)
		}
		rows.Close()
		if _, err := db.Exec(`ALTER TABLE ` + table + ` RENAME TO fixture_` + table); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE VIEW ` + table + ` AS SELECT ` + strings.Join(columns, ",") + ` FROM fixture_` + table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.LoadConversationSnapshot(ctx, conv.ID); err == nil {
		t.Fatal("full reader unexpectedly accepted hazardous body columns")
	}
	turns, err := s.LoadConversationLiveTurns(ctx, conv.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(turns) != 1 || turns[0].ID != "live-turn" || turns[0].ProviderTurnID != "native-running" || turns[0].Plan != nil || turns[0].Diff != nil || turns[0].ErrorMessage != "" {
		t.Fatalf("unexpected control projection: %+v", turns)
	}
}

func TestLiveTurnReaderPreservesVisibleBranchAndProviderScope(t *testing.T) {
	ctx := context.Background()
	s, session, conv := seededChatConversation(t)
	seedBranchTurns(t, s, session, conv)
	child := domain.ConversationBranch{ID: "live-child", ConversationID: conv.ID, SessionID: session.ID, ProviderConversationID: "new-thread", ParentBranchID: conv.ActiveBranchID, ForkAfterSequence: 2, ProviderScopeID: "new-scope", CreatedAt: testNow}
	if err := s.CreateAndActivateConversationBranch(ctx, session.ID, child, "new-generation", testNow); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppendUserMessage(ctx, conv.ID, session.ID, "new-generation", domain.ConversationMessage{ID: "child-user", Origin: domain.MessageOriginHuman, Text: "child"}, "child-turn", testNow); err != nil {
		t.Fatal(err)
	}
	if err := s.BindTurnToProvider(ctx, "child-turn", "child-native", testNow); err != nil {
		t.Fatal(err)
	}
	turns, err := s.LoadConversationLiveTurns(ctx, conv.ID)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, turn := range turns {
		seen[turn.ID] = turn.ProviderTurnID
	}
	if len(seen) != 2 || seen["turn-1"] != "" || seen["child-turn"] != "child-native" {
		t.Fatalf("lineage/provider scope: %+v", seen)
	}
	if _, ok := seen["turn-2"]; ok {
		t.Fatal("discarded lineage turn visible")
	}
}

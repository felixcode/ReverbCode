package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	_ "modernc.org/sqlite"
)

func TestRedactionNativeIndexesAndEquivalentPredicates(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	schema := `CREATE TABLE conversation_turns(id TEXT PRIMARY KEY,conversation_id TEXT,provider_turn_id TEXT,state TEXT);
 CREATE UNIQUE INDEX idx_conversation_turns_provider ON conversation_turns(conversation_id,provider_turn_id) WHERE provider_turn_id<>'';
 CREATE TABLE conversation_messages(id TEXT PRIMARY KEY,turn_id TEXT,conversation_id TEXT,provider_item_id TEXT,client_message_id TEXT);
 CREATE UNIQUE INDEX idx_conversation_messages_provider_item ON conversation_messages(conversation_id,provider_item_id) WHERE provider_item_id<>'';
 CREATE UNIQUE INDEX idx_conversation_messages_client_id ON conversation_messages(conversation_id,client_message_id) WHERE client_message_id<>'';
 CREATE TABLE conversation_activities(id TEXT PRIMARY KEY,turn_id TEXT,conversation_id TEXT,provider_item_id TEXT);
 CREATE UNIQUE INDEX idx_conversation_activities_provider_item ON conversation_activities(conversation_id,provider_item_id) WHERE provider_item_id<>'';
 CREATE TABLE ao_history_retention(object_kind TEXT,object_id TEXT,PRIMARY KEY(object_kind,object_id));
 INSERT INTO conversation_turns VALUES('t','c','native','completed'),('live','c','running','running');
 INSERT INTO conversation_messages VALUES('m','t','c','item','client');
 INSERT INTO conversation_activities VALUES('a','t','c','tool');
 INSERT INTO ao_history_retention VALUES('turn','t'),('turn','live'),('message','m'),('activity','a');`
	if _, err = db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	old := expiredProviderPayloadSQL
	for _, column := range []string{"t.provider_turn_id", "m.provider_item_id", "m.client_message_id", "a.provider_item_id"} {
		old = strings.Replace(old, " AND "+column+"<>''", "", 1)
	}
	for _, conv := range []string{"c", "other"} {
		for _, native := range []string{"", "native", "running", "missing"} {
			for _, item := range []string{"", "item", "tool", "missing"} {
				for _, client := range []string{"", "client", "missing"} {
					args := []any{native, conv, native, item, conv, item, client, conv, client, item, conv, item}
					var before, after bool
					if err := db.QueryRow(old, args...).Scan(&before); err != nil {
						t.Fatal(err)
					}
					if err := db.QueryRow(expiredProviderPayloadSQL, args...).Scan(&after); err != nil {
						t.Fatal(err)
					}
					if before != after {
						t.Fatal("predicate semantic mismatch")
					}
				}
			}
		}
	}
	args := []any{"native", "c", "native", "item", "c", "item", "client", "c", "client", "tool", "c", "tool"}
	rows, err := db.Query("EXPLAIN QUERY PLAN "+expiredProviderPayloadSQL, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plans []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plans = append(plans, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(plans, "\n")
	for _, index := range []string{"idx_conversation_turns_provider", "idx_conversation_messages_provider_item", "idx_conversation_messages_client_id", "idx_conversation_activities_provider_item"} {
		if !strings.Contains(plan, index) {
			t.Fatalf("modernc missing partial index %s: %s", index, plan)
		}
	}
}

func TestArchivedIDsExactGenerationAndProjectionTransaction(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`CREATE TABLE sessions(id TEXT PRIMARY KEY,controller_generation TEXT);INSERT INTO sessions VALUES('s','g');CREATE TABLE review(id TEXT PRIMARY KEY,controller_generation TEXT,session_id TEXT,interface_mode TEXT);INSERT INTO review VALUES('r','g','s','chat');CREATE TABLE conversation_provider_events(conversation_id TEXT,provider_event_id TEXT);CREATE UNIQUE INDEX idx_conversation_provider_events_dedupe ON conversation_provider_events(conversation_id,provider_event_id) WHERE provider_event_id<>'';INSERT INTO conversation_provider_events VALUES('c','id:suffix'),('other','id:new');`); err != nil {
		t.Fatal(err)
	}
	s := NewStore(db, db)
	ctx := context.Background()
	known, err := s.ArchivedProviderEventIDs(ctx, "c", domain.SessionID("s"), "", "g", []string{"id:suffix", "id:new", "id", ""})
	if err != nil {
		t.Fatal(err)
	}
	if len(known) != 1 || !known["id:suffix"] {
		t.Fatalf("not exact identity: %+v", known)
	}
	if _, err := s.ArchivedProviderEventIDs(ctx, "c", "s", "", "stale", []string{"id:suffix"}); err == nil {
		t.Fatal("stale generation passed")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`INSERT INTO conversation_provider_events VALUES('c','pending')`); err != nil {
		t.Fatal(err)
	}
	bound := context.WithValue(ctx, projectionDBKey{}, tx)
	known, err = s.ArchivedProviderEventIDs(bound, "c", "s", "r", "g", []string{"pending"})
	if err != nil || !known["pending"] {
		t.Fatalf("transaction was not reused: %+v %v", known, err)
	}
	if _, err := s.ArchivedProviderEventIDs(bound, "c", "s", "", "g", make([]string, 257)); err == nil {
		t.Fatal("unbounded batch passed")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	known, err = s.ArchivedProviderEventIDs(ctx, "c", "s", "", "g", []string{"pending"})
	if err != nil || known["pending"] {
		t.Fatal("rolled back identity leaked into next import")
	}
}

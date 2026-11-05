package store

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
	_ "modernc.org/sqlite"
)

func providerItemFixture(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`
CREATE TABLE conversation_messages(
 id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,turn_id TEXT,sequence INTEGER NOT NULL DEFAULT 0,
 revision INTEGER NOT NULL DEFAULT 0,role TEXT NOT NULL DEFAULT 'assistant',origin TEXT NOT NULL DEFAULT 'provider',
 text TEXT NOT NULL DEFAULT 'old',streaming INTEGER NOT NULL DEFAULT 0,provider_item_id TEXT NOT NULL DEFAULT '',
 client_message_id TEXT NOT NULL DEFAULT '',created_at TIMESTAMP NOT NULL DEFAULT '2026-10-01 00:00:00',
 updated_at TIMESTAMP NOT NULL DEFAULT '2026-10-01 00:00:00',delivery_content_json TEXT NOT NULL DEFAULT '[]',
 branch_id TEXT NOT NULL DEFAULT '',client_payload_hash TEXT NOT NULL DEFAULT '',sender_session_id TEXT NOT NULL DEFAULT '',
 sender_project_id TEXT NOT NULL DEFAULT '',sender_display_name TEXT NOT NULL DEFAULT '');
CREATE UNIQUE INDEX idx_conversation_messages_provider_item ON conversation_messages(conversation_id,provider_item_id) WHERE provider_item_id<>'';
CREATE INDEX idx_conversation_messages_order ON conversation_messages(conversation_id,sequence);
CREATE TABLE conversation_activities(id TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,provider_item_id TEXT NOT NULL DEFAULT '',
 sequence INTEGER NOT NULL DEFAULT 0,kind TEXT NOT NULL DEFAULT 'command',status TEXT NOT NULL DEFAULT 'running',
 summary TEXT NOT NULL DEFAULT 'keep',detail_json TEXT NOT NULL DEFAULT '{"keep":1}',revision INTEGER NOT NULL DEFAULT 0,
 command_output TEXT NOT NULL DEFAULT 'old',command_output_truncated INTEGER NOT NULL DEFAULT 0,
 streamed_text TEXT NOT NULL DEFAULT 'old',streamed_text_truncated INTEGER NOT NULL DEFAULT 0,
 updated_at TIMESTAMP NOT NULL DEFAULT '2026-10-01 00:00:00');
CREATE UNIQUE INDEX idx_conversation_activities_provider_item ON conversation_activities(conversation_id,provider_item_id) WHERE provider_item_id<>'';
CREATE INDEX idx_conversation_activities_order ON conversation_activities(conversation_id,sequence);
CREATE TABLE ao_history_retention(object_kind TEXT,object_id TEXT,PRIMARY KEY(object_kind,object_id));
INSERT INTO conversation_messages(id,conversation_id,provider_item_id,sequence) VALUES
 ('hit','c','item',1),('empty1','c','',2),('empty2','c','',3),('foreign','other','item',1),('different','c','other',4);
INSERT INTO conversation_activities(id,conversation_id,provider_item_id,sequence) VALUES
 ('hit','c','item',1),('empty1','c','',2),('empty2','c','',3),('foreign','other','item',1),('different','c','other',4),
 ('cancelled','c','cancelled',5),('capped','c','capped',6);
UPDATE conversation_activities SET status='cancelled' WHERE id='cancelled';
UPDATE conversation_activities SET command_output_truncated=1,streamed_text_truncated=1 WHERE id='capped';
`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

var providerItemOperations = []string{"message lookup", "message append", "message settle", "activity output", "activity text", "activity exists"}

func runProviderItemOperation(ctx context.Context, q *gen.Queries, name, conv, item string, indexed bool) (any, error) {
	now := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
	switch name {
	case "message lookup":
		a := gen.SelectConversationMessageByProviderItemParams{ConversationID: conv, ProviderItemID: item}
		if indexed {
			return providerItemSelectConversationMessageByProviderItem(ctx, q, a)
		}
		return q.SelectConversationMessageByProviderItem(ctx, a)
	case "message append":
		a := gen.AppendConversationMessageDeltaParams{Text: "Δ next", UpdatedAt: now, ConversationID: conv, ProviderItemID: item}
		if indexed {
			return nil, providerItemAppendConversationMessageDelta(ctx, q, a)
		}
		return nil, q.AppendConversationMessageDelta(ctx, a)
	case "message settle":
		a := gen.SettleConversationMessageParams{Text: "settled Δ", UpdatedAt: now, ConversationID: conv, ProviderItemID: item}
		if indexed {
			return nil, providerItemSettleConversationMessage(ctx, q, a)
		}
		return nil, q.SettleConversationMessage(ctx, a)
	case "activity output":
		a := gen.AppendConversationActivityOutputParams{Delta: "Δ next", MaxOutputChars: 9, UpdatedAt: now, ConversationID: conv, ProviderItemID: item}
		if indexed {
			return providerItemAppendConversationActivityOutput(ctx, q, a)
		}
		return q.AppendConversationActivityOutput(ctx, a)
	case "activity text":
		a := gen.AppendConversationActivityStreamedTextParams{Delta: "Δ next", MaxTextChars: 9, UpdatedAt: now, ConversationID: conv, ProviderItemID: item}
		if indexed {
			return providerItemAppendConversationActivityStreamedText(ctx, q, a)
		}
		return q.AppendConversationActivityStreamedText(ctx, a)
	case "activity exists":
		a := gen.ConversationActivityExistsForProviderItemParams{ConversationID: conv, ProviderItemID: item}
		if indexed {
			return providerItemConversationActivityExistsForProviderItem(ctx, q, a)
		}
		return q.ConversationActivityExistsForProviderItem(ctx, a)
	}
	panic(name)
}

func providerItemRows(t *testing.T, db *sql.DB, table string) [][]any {
	t.Helper()
	rows, err := db.Query("SELECT * FROM " + table + " ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result [][]any
	for rows.Next() {
		values := make([]any, len(columns))
		ptr := make([]any, len(columns))
		for i := range ptr {
			ptr[i] = &values[i]
		}
		if err := rows.Scan(ptr...); err != nil {
			t.Fatal(err)
		}
		result = append(result, values)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestProviderItemRoutesPreserveOriginalResultsAndAllFields(t *testing.T) {
	for _, name := range providerItemOperations {
		for _, conv := range []string{"c", "other", "missing"} {
			for _, item := range []string{"item", "", "missing", "cancelled", "capped"} {
				for _, expired := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/expired=%v", name, conv, item, expired), func(t *testing.T) {
						old, updated := providerItemFixture(t), providerItemFixture(t)
						if expired {
							for _, db := range []*sql.DB{old, updated} {
								if _, err := db.Exec("INSERT INTO ao_history_retention VALUES('message','hit'),('activity_output','hit')"); err != nil {
									t.Fatal(err)
								}
							}
						}
						a, ae := runProviderItemOperation(context.Background(), gen.New(old), name, conv, item, false)
						b, be := runProviderItemOperation(context.Background(), gen.New(updated), name, conv, item, true)
						if fmt.Sprint(ae) != fmt.Sprint(be) || !reflect.DeepEqual(a, b) {
							t.Fatalf("result changed: %v/%v vs %v/%v", a, ae, b, be)
						}
						for _, table := range []string{"conversation_messages", "conversation_activities"} {
							if !reflect.DeepEqual(providerItemRows(t, old, table), providerItemRows(t, updated, table)) {
								t.Fatalf("%s fields changed", table)
							}
						}
					})
				}
			}
		}
	}
}

type providerItemCapture struct {
	*sql.DB
	query string
	args  []any
}

func (c *providerItemCapture) ExecContext(ctx context.Context, q string, a ...any) (sql.Result, error) {
	c.query = q
	c.args = a
	return c.DB.ExecContext(ctx, q, a...)
}
func (c *providerItemCapture) QueryRowContext(ctx context.Context, q string, a ...any) *sql.Row {
	c.query = q
	c.args = a
	return c.DB.QueryRowContext(ctx, q, a...)
}

func TestProviderItemNonemptyRoutesSeekExistingPartialIndexes(t *testing.T) {
	db := providerItemFixture(t)
	for _, name := range providerItemOperations {
		t.Run(name, func(t *testing.T) {
			c := &providerItemCapture{DB: db}
			if _, err := runProviderItemOperation(context.Background(), gen.New(c), name, "c", "item", true); err != nil {
				t.Fatal(err)
			}
			rows, err := db.Query("EXPLAIN QUERY PLAN "+c.query, c.args...)
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
			table := "messages"
			if strings.HasPrefix(name, "activity") {
				table = "activities"
			}
			plan := strings.Join(plans, "\n")
			if !strings.Contains(plan, "idx_conversation_"+table+"_provider_item (conversation_id=? AND provider_item_id=?)") || strings.Contains(plan, "idx_conversation_"+table+"_order") {
				t.Fatalf("not a provider-item point seek: %s", plan)
			}
		})
	}
	c := &providerItemCapture{DB: db}
	if _, err := gen.New(c).SelectConversationContextResetSequence(context.Background(), gen.SelectConversationContextResetSequenceParams{ConversationID: "c", ProviderItemID: "ao-context-reset:session"}); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("EXPLAIN QUERY PLAN "+c.query, c.args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan string
	for rows.Next() {
		var a, b, c int
		var d string
		if err := rows.Scan(&a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		plan += d
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "idx_conversation_activities_provider_item (conversation_id=? AND provider_item_id=?)") {
		t.Fatal(plan)
	}
}

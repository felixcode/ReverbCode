package store_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	sqliteDriver "modernc.org/sqlite"
)

const legacyActivityLookup = `SELECT id FROM conversation_activities
WHERE conversation_id = ? AND provider_item_id = ? LIMIT 1;`

const legacyActivitySettlement = `UPDATE conversation_activities
SET status = ?, summary = ?, detail_json = ?, revision = revision + 1, updated_at = ?
WHERE conversation_id = ? AND provider_item_id = ? AND status <> 'cancelled';`

const legacyActivityStreamedSettlement = `UPDATE conversation_activities
SET streamed_text = ?, streamed_text_truncated = 0, revision = revision + 1, updated_at = ?
WHERE conversation_id = ? AND provider_item_id = ? AND status <> 'cancelled';`

var activityCandidateProbe struct {
	once  sync.Once
	calls atomic.Int64
	err   error
}

func activitySettlementFixture(t *testing.T) (*sql.DB, string, string, string) {
	t.Helper()
	activityCandidateProbe.once.Do(func() {
		activityCandidateProbe.err = sqliteDriver.RegisterScalarFunction("activity_candidate_probe", 1,
			func(_ *sqliteDriver.FunctionContext, args []driver.Value) (driver.Value, error) {
				activityCandidateProbe.calls.Add(1)
				return args[0], nil
			})
	})
	if activityCandidateProbe.err != nil {
		t.Fatal(activityCandidateProbe.err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	// Indexes mirror the durable activity table. The update trigger accounts for
	// every revision change, including otherwise identical repeated settlements.
	_, err = db.Exec(`
CREATE TABLE conversation_activities(id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL,
 provider_item_id TEXT NOT NULL DEFAULT '', sequence INTEGER, kind TEXT, status TEXT,
 summary TEXT, detail_json TEXT, revision INTEGER, updated_at INTEGER,
 branch_id TEXT, turn_id TEXT, request_id TEXT, created_at INTEGER,
 command_output TEXT, streamed_text TEXT, streamed_text_truncated INTEGER DEFAULT 0);
CREATE UNIQUE INDEX idx_conversation_activities_provider_item
 ON conversation_activities(conversation_id, provider_item_id) WHERE provider_item_id <> '';
CREATE INDEX idx_conversation_activities_order ON conversation_activities(conversation_id, sequence);
CREATE INDEX idx_conversation_activities_pending ON conversation_activities(conversation_id, kind, status);
CREATE INDEX idx_conversation_activities_branch ON conversation_activities(branch_id, sequence);
CREATE TABLE ao_history_retention(object_kind TEXT, object_id TEXT, PRIMARY KEY(object_kind, object_id));
CREATE TABLE changes(activity_id TEXT, revision INTEGER, updated_at INTEGER);
CREATE TRIGGER activity_revision_update AFTER UPDATE ON conversation_activities
 WHEN OLD.revision <> NEW.revision BEGIN
 INSERT INTO changes VALUES(NEW.id, NEW.revision, NEW.updated_at);
END;`)
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../queries/conversations.sql")
	if err != nil {
		t.Fatal(err)
	}
	readQuery := func(name string) string {
		t.Helper()
		_, query, found := strings.Cut(string(source), "-- name: "+name+" :")
		if !found {
			t.Fatalf("query %s missing", name)
		}
		_, query, _ = strings.Cut(query, "\n")
		query, _, _ = strings.Cut(query, "-- name:")
		lines := strings.Split(query, "\n")
		for i, line := range lines {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				lines[i] = ""
			}
		}
		query = strings.Join(lines, "\n")
		query, _, _ = strings.Cut(query, ";")
		return query + ";"
	}
	lookup := strings.Replace(readQuery("SelectConversationActivityByProviderItem"), "SELECT *", "SELECT id", 1)
	return db, lookup, readQuery("SettleConversationActivity"), readQuery("SettleConversationActivityStreamedText")
}

func activityQueryState(t *testing.T, db *sql.DB) string {
	t.Helper()
	var activities, changes string
	err := db.QueryRow(`SELECT json_group_array(json_array(id, conversation_id, provider_item_id,
 sequence, kind, status, summary, detail_json, revision, updated_at, branch_id, turn_id,
 request_id, created_at, command_output, streamed_text, streamed_text_truncated)) FROM
 (SELECT * FROM conversation_activities ORDER BY id)`).Scan(&activities)
	if err != nil {
		t.Fatal(err)
	}
	err = db.QueryRow(`SELECT json_group_array(json_array(activity_id, revision, updated_at)) FROM
 (SELECT * FROM changes ORDER BY rowid)`).Scan(&changes)
	if err != nil {
		t.Fatal(err)
	}
	return activities + "\n" + changes
}

func TestActivitySettlementQueryPreservesTruth(t *testing.T) {
	for _, tc := range []struct {
		name, mutation string
		repeat         int
	}{
		{name: "running"},
		{name: "completed", mutation: `UPDATE conversation_activities SET status='completed' WHERE id='activity'`},
		{name: "failed", mutation: `UPDATE conversation_activities SET status='failed' WHERE id='activity'`},
		{name: "pending", mutation: `UPDATE conversation_activities SET status='pending' WHERE id='activity'`},
		{name: "resolved", mutation: `UPDATE conversation_activities SET status='resolved' WHERE id='activity'`},
		{name: "cancelled", mutation: `UPDATE conversation_activities SET status='cancelled' WHERE id='activity'`},
		{name: "other-conversation", mutation: `UPDATE conversation_activities SET conversation_id='other' WHERE id='activity'`},
		{name: "other-item", mutation: `UPDATE conversation_activities SET provider_item_id='other' WHERE id='activity'`},
		{name: "empty-stored-item", mutation: `UPDATE conversation_activities SET provider_item_id='' WHERE id='activity'`},
		{name: "missing", mutation: `DELETE FROM conversation_activities WHERE id='activity'`},
		{name: "item-case-sensitive", mutation: `UPDATE conversation_activities SET provider_item_id='Item' WHERE id='activity'`},
		{name: "other-branch", mutation: `UPDATE conversation_activities SET branch_id='other' WHERE id='activity'`},
		{name: "other-turn", mutation: `UPDATE conversation_activities SET turn_id='other' WHERE id='activity'`},
		{name: "repeated-settlement", repeat: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want string
			for _, version := range []string{"legacy", "indexed"} {
				db, lookup, settle, stream := activitySettlementFixture(t)
				_, err := db.Exec(`INSERT INTO conversation_activities VALUES
 ('activity','conversation','item',1,'command','running','before','{}',7,1,'branch','turn','request',0,'output','reasoning',1),
 ('neighbor','conversation','',2,'reasoning','running','anonymous','{}',3,1,'branch','turn','',0,'','',0),
 ('foreign','foreign','item',1,'command','running','foreign','{}',0,1,'foreign','foreign','',0,'','',0);`)
				if err != nil {
					t.Fatal(err)
				}
				if tc.mutation != "" {
					if _, err := db.Exec(tc.mutation); err != nil {
						t.Fatal(err)
					}
				}
				if version == "legacy" {
					lookup, settle, stream = legacyActivityLookup, legacyActivitySettlement, legacyActivityStreamedSettlement
				}
				var id string
				err = db.QueryRow(lookup, "conversation", "item").Scan(&id)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					t.Fatal(err)
				}
				for range max(1, tc.repeat) {
					if _, err := db.Exec(settle, "completed", "after", `{"settled":true}`, 2, "conversation", "item"); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := db.Exec(stream, "settled prose", 3, "conversation", "item"); err != nil {
					t.Fatal(err)
				}
				got := id + "\n" + activityQueryState(t, db)
				if version == "legacy" {
					want = got
				} else if got != want {
					t.Fatalf("lookup/activity/CDC state differs from legacy:\n%s\nwant:\n%s", got, want)
				}
			}
		})
	}
}

func TestActivitySettlementReplayUsesProviderItemIndex(t *testing.T) {
	for _, size := range []int{128, 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			counts := make(map[string]int64)
			for _, version := range []string{"legacy", "indexed"} {
				db, lookup, settle, stream := activitySettlementFixture(t)
				tx, err := db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				for i := range size {
					if _, err := tx.Exec(`INSERT INTO conversation_activities(id,conversation_id,
 provider_item_id,sequence,kind,status,revision) VALUES(?,'conversation',?,?,'command','running',0)`,
						fmt.Sprint(i), fmt.Sprintf("item-%d", i), i); err != nil {
						t.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				if version == "legacy" {
					lookup, settle, stream = legacyActivityLookup, legacyActivitySettlement, legacyActivityStreamedSettlement
				} else {
					for _, query := range []struct {
						sql  string
						args []any
					}{
						{lookup, []any{"conversation", "item-0"}},
						{settle, []any{"completed", "settled", "{}", 2, "conversation", "item-0"}},
						{stream, []any{"settled prose", 3, "conversation", "item-0"}},
					} {
						rows, err := db.Query("EXPLAIN QUERY PLAN "+query.sql, query.args...)
						if err != nil {
							t.Fatal(err)
						}
						var indexed bool
						for rows.Next() {
							var id, parent, unused int
							var detail string
							if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
								t.Fatal(err)
							}
							indexed = indexed || strings.Contains(detail, "idx_conversation_activities_provider_item (conversation_id=? AND provider_item_id=?)")
						}
						if err := rows.Err(); err != nil {
							t.Fatal(err)
						}
						_ = rows.Close()
						if !indexed {
							t.Fatal("activity query does not seek the two-column provider-item index")
						}
					}
				}
				// Count candidate visits without wrapping either indexed key, so the
				// real predicates and partial-index eligibility remain intact.
				instrument := func(query string) string {
					for _, prefix := range []string{"WHERE conversation_id = ?", "WHERE conversation_activities.conversation_id = ?"} {
						if strings.Contains(query, prefix) {
							return strings.Replace(query, prefix, prefix+" AND activity_candidate_probe(1) = 1", 1)
						}
					}
					t.Fatal("activity probe did not match the source predicate")
					return query
				}
				lookup, settle, stream = instrument(lookup), instrument(settle), instrument(stream)
				activityCandidateProbe.calls.Store(0)
				for i := range size {
					item := fmt.Sprintf("item-%d", i)
					var id string
					if err := db.QueryRow(lookup, "conversation", item).Scan(&id); err != nil || id != fmt.Sprint(i) {
						t.Fatalf("lookup %s = %q, %v", item, id, err)
					}
					if _, err := db.Exec(settle, "completed", "settled", "{}", 2, "conversation", item); err != nil {
						t.Fatal(err)
					}
					if _, err := db.Exec(stream, "settled prose", 3, "conversation", item); err != nil {
						t.Fatal(err)
					}
				}
				counts[version] = activityCandidateProbe.calls.Load()
			}
			if counts["legacy"] < int64(size*size) || counts["indexed"] != int64(3*size) {
				t.Fatalf("activity replay candidate visits for %d rows: %v", size, counts)
			}
			t.Logf("activities=%d legacy_candidate_visits=%d indexed_candidate_visits=%d", size, counts["legacy"], counts["indexed"])
		})
	}
}

func TestActivitySettlementProjectionKeepsIdentityCDCAndIdempotence(t *testing.T) {
	s, session, conversation := conversationFixture(t)
	ctx := context.Background()
	seedTurn(t, s, conversation, session, "original", "original", histClock)
	seedTurn(t, s, conversation, session, "other", "other", histClock.Add(time.Second))
	before, err := s.LoadConversationSnapshot(ctx, conversation)
	if err != nil {
		t.Fatal(err)
	}
	original := before.Activities[0]
	seq, err := s.LatestSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	imported := domain.ConversationActivity{ID: "replacement-id", ProviderItemID: original.ProviderItemID,
		Kind: domain.ActivityKindReasoning, Status: domain.ActivityStatusRunning,
		Summary: "imported", Detail: []byte(`{"imported":true}`), RequestID: "replacement-request"}
	project := func(txCtx context.Context) error {
		return s.UpsertActivity(txCtx, conversation, "provider-other", imported, histClock.Add(2*time.Second))
	}
	for i := range 2 {
		applied, err := s.ProjectProviderEvent(ctx, conversation, session, "gen-1", "history-activity",
			"activity.started", `{}`, histClock.Add(2*time.Second), project)
		if err != nil || applied != (i == 0) {
			t.Fatalf("project %d: applied=%v err=%v", i, applied, err)
		}
	}
	after, err := s.LoadConversationSnapshot(ctx, conversation)
	if err != nil {
		t.Fatal(err)
	}
	want := original
	want.Status, want.Summary, want.Detail = imported.Status, imported.Summary, imported.Detail
	want.Revision++
	want.UpdatedAt = histClock.Add(2 * time.Second)
	if len(after.Activities) != len(before.Activities) || !reflect.DeepEqual(after.Activities[0], want) {
		t.Fatalf("settlement changed identity/turn/sequence/kind/request/creation or replay count: %+v, want %+v", after.Activities, want)
	}
	events, err := s.EventsAfter(ctx, seq, 10)
	if err != nil || len(events) != 1 || events[0].SessionID != string(session) || string(events[0].Type) != "session_updated" {
		t.Fatalf("settlement CDC = %+v, %v", events, err)
	}
	if applied, err := s.ProjectProviderEvent(ctx, conversation, session, "stale-generation", "stale-activity",
		"activity.started", `{}`, histClock, project); err != nil || applied {
		t.Fatalf("stale generation settlement applied=%v err=%v", applied, err)
	}
	projectionErr := errors.New("projection failed after settlement")
	_, err = s.ProjectProviderEvent(ctx, conversation, session, "gen-1", "failed-activity", "activity.started", `{}`,
		histClock, func(txCtx context.Context) error {
			imported.Summary = "must roll back"
			if err := s.UpsertActivity(txCtx, conversation, "provider-other", imported, histClock); err != nil {
				return err
			}
			return projectionErr
		})
	if !errors.Is(err, projectionErr) {
		t.Fatalf("projection error = %v", err)
	}
	rolledBack, err := s.LoadConversationSnapshot(ctx, conversation)
	if err != nil || !reflect.DeepEqual(rolledBack.Activities, after.Activities) {
		t.Fatalf("settlement escaped rollback: %+v, %v", rolledBack.Activities, err)
	}
	archive, err := s.ProviderEventsSince(ctx, conversation, 0, 10)
	if err != nil || len(archive) != 1 {
		t.Fatalf("archive after duplicate/stale/rolled-back settlement = %+v, %v", archive, err)
	}
	events, err = s.EventsAfter(ctx, seq, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("duplicate/stale/rolled-back settlement emitted CDC = %+v, %v", events, err)
	}
}

func TestActivitySettlementEmptyAndCancelledItemsPreserveStoreBehavior(t *testing.T) {
	s, _, conversation := conversationFixture(t)
	ctx := context.Background()
	for _, id := range []string{"anonymous-1", "anonymous-2"} {
		if err := s.UpsertActivity(ctx, conversation, "", domain.ConversationActivity{
			ID: id, Kind: domain.ActivityKindSystem, Status: domain.ActivityStatusCompleted, Summary: id,
		}, histClock); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.UpsertActivity(ctx, conversation, "", domain.ConversationActivity{
		ID: "cancelled", ProviderItemID: "cancelled-item", Kind: domain.ActivityKindCommand,
		Status: domain.ActivityStatusCancelled, Summary: "cancelled",
	}, histClock); err != nil {
		t.Fatal(err)
	}
	before, err := s.LoadConversationSnapshot(ctx, conversation)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.UpsertActivity(ctx, conversation, "", domain.ConversationActivity{
		ID: "replacement", ProviderItemID: "cancelled-item", Kind: domain.ActivityKindCommand,
		Status: domain.ActivityStatusCompleted, Summary: "must stay cancelled",
	}, histClock.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if found, err := s.SettleActivityStreamedText(ctx, conversation, "", "must not change anonymous items", histClock); err != nil || found {
		t.Fatalf("empty item streamed settlement found=%v err=%v", found, err)
	}
	if found, err := s.SettleActivityStreamedText(ctx, conversation, "cancelled-item", "must stay cancelled", histClock); err != nil || found {
		t.Fatalf("cancelled item streamed settlement found=%v err=%v", found, err)
	}
	after, err := s.LoadConversationSnapshot(ctx, conversation)
	if err != nil || len(after.Activities) != 3 || !reflect.DeepEqual(after.Activities, before.Activities) {
		t.Fatalf("anonymous items merged or cancelled item changed: %+v, %v", after.Activities, err)
	}
}

func TestActivitySettlementHistoryReplayScaleIsIdempotent(t *testing.T) {
	const size = 3933
	s, session, conversation := conversationFixture(t)
	ctx := context.Background()
	if _, err := s.AppendUserMessage(ctx, conversation, session, "gen-1", domain.ConversationMessage{
		ID: "synthetic-history-user", Text: "synthetic history", Origin: domain.MessageOriginHuman,
	}, "history-turn", histClock); err != nil {
		t.Fatal(err)
	}
	if err := s.BindTurnToProvider(ctx, "history-turn", "provider-history", histClock); err != nil {
		t.Fatal(err)
	}
	for i := range size {
		if err := s.UpsertActivity(ctx, conversation, "provider-history", domain.ConversationActivity{
			ID: fmt.Sprintf("native-%d", i), ProviderItemID: fmt.Sprintf("item-%d", i),
			Kind: domain.ActivityKindReasoning, Status: domain.ActivityStatusCompleted, Summary: "native",
		}, histClock); err != nil {
			t.Fatal(err)
		}
	}
	before, err := s.LoadConversationSnapshot(ctx, conversation)
	if err != nil || len(before.Activities) != size {
		t.Fatalf("seeded history activities=%d err=%v", len(before.Activities), err)
	}
	seq, err := s.LatestSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var firstReplay []domain.ConversationActivity
	var appliedCount int
	for replay := range 2 {
		for i := range size {
			for _, stage := range []string{"started", "completed"} {
				item := fmt.Sprintf("item-%d", i)
				status := domain.ActivityStatusRunning
				if stage == "completed" {
					status = domain.ActivityStatusCompleted
				}
				applied, err := s.ProjectProviderEvent(ctx, conversation, session, "gen-1",
					fmt.Sprintf("history-%d-%s", i, stage), "activity."+stage, `{}`, histClock.Add(time.Second),
					func(txCtx context.Context) error {
						if err := s.UpsertActivity(txCtx, conversation, "provider-history", domain.ConversationActivity{
							ID: "replay-" + item, ProviderItemID: item, Kind: domain.ActivityKindReasoning,
							Status: status, Summary: "imported",
						}, histClock.Add(time.Second)); err != nil {
							return err
						}
						if stage == "completed" {
							_, err := s.SettleActivityStreamedText(txCtx, conversation, item, "settled reasoning", histClock.Add(time.Second))
							return err
						}
						return nil
					})
				if err != nil || applied != (replay == 0) {
					t.Fatalf("history replay=%d item=%d stage=%s applied=%v err=%v", replay, i, stage, applied, err)
				}
				if applied {
					appliedCount++
				}
			}
		}
		snapshot, err := s.LoadConversationSnapshot(ctx, conversation)
		if err != nil || len(snapshot.Activities) != size {
			t.Fatalf("replayed history activities=%d err=%v", len(snapshot.Activities), err)
		}
		for i, got := range snapshot.Activities {
			want := before.Activities[i]
			want.Summary, want.StreamedText = "imported", "settled reasoning"
			want.UpdatedAt, want.Revision = histClock.Add(time.Second), want.Revision+3
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("history item %d changed identity/branch/turn/sequence or settled facts: %+v, want %+v", i, got, want)
			}
		}
		if replay == 0 {
			firstReplay = snapshot.Activities
		} else if !reflect.DeepEqual(snapshot.Activities, firstReplay) {
			t.Fatal("duplicate native-history replay changed activity state")
		}
	}
	archive, err := s.ProviderEventsSince(ctx, conversation, 0, 2*size+1)
	if err != nil || len(archive) != 2*size || appliedCount != 2*size {
		t.Fatalf("history archive=%d applied=%d err=%v", len(archive), appliedCount, err)
	}
	events, err := s.EventsAfter(ctx, seq, 3*size+1)
	if err != nil || len(events) != 3*size {
		t.Fatalf("history CDC=%d err=%v, want %d", len(events), err, 3*size)
	}
	t.Logf("activities=%d imported_events=%d duplicate_events=%d CDC=%d", size, appliedCount, 2*size, len(events))
}

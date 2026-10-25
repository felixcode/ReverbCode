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
)

func providerTurnFixture(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	// Source-qualified production table shape; foreign keys stay disabled only in
	// this synthetic isolated database. No application migration or row is altered.
	if _, err = db.Exec(`CREATE TABLE "conversation_turns" (
    id                     TEXT PRIMARY KEY,
    conversation_id        TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    handled_by_session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    provider_turn_id       TEXT NOT NULL DEFAULT '',
    controller_generation  TEXT NOT NULL DEFAULT '',
    state                  TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'recovered', 'interrupted', 'failed', 'cancelled')),
    error_message          TEXT NOT NULL DEFAULT '',
    requested_at           TIMESTAMP NOT NULL,
    started_at             TIMESTAMP,
    completed_at           TIMESTAMP,
    diff_json              TEXT NOT NULL DEFAULT '',
    rolled_back_at         TIMESTAMP,
    plan_json              TEXT NOT NULL DEFAULT '',
    branch_id              TEXT NOT NULL DEFAULT '',
    promotion_started_at   TIMESTAMP,
    promoted_to_turn_id    TEXT REFERENCES "conversation_turns"(id) ON DELETE SET NULL,
    retry_of_turn_id       TEXT REFERENCES "conversation_turns"(id) ON DELETE RESTRICT
, handled_by_review_id TEXT REFERENCES review(id) ON DELETE SET NULL);
CREATE UNIQUE INDEX idx_conversation_turns_provider ON conversation_turns(conversation_id,provider_turn_id) WHERE provider_turn_id<>'';
CREATE INDEX idx_conversation_turns_conversation ON conversation_turns(conversation_id,requested_at);
CREATE TABLE ao_history_retention(object_kind TEXT,object_id TEXT,PRIMARY KEY(object_kind,object_id));
`); err != nil {
		t.Fatal(err)
	}
	for i, r := range []struct{ id, conv, provider, state string }{
		{"hit", "c", "provider", "completed"}, {"empty1", "c", "", "queued"}, {"empty2", "c", "", "running"},
		{"foreign", "other", "provider", "completed"}, {"failed", "c", "failed", "failed"}, {"running", "c", "running", "running"},
	} {
		if _, err = db.Exec("INSERT INTO conversation_turns(id,conversation_id,handled_by_session_id,provider_turn_id,state,requested_at,diff_json,plan_json) VALUES(?,?,'s',?,?,?,?,?)", r.id, r.conv, r.provider, r.state, time.Date(2026, 10, 1, 0, 0, i, 0, time.UTC), `{"keep":"diff"}`, `{"keep":"plan"}`); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func runProviderTurnOperation(ctx context.Context, q *gen.Queries, name, conv, provider string, indexed bool) (any, error) {
	switch name {
	case "lookup":
		a := gen.SelectConversationTurnByProviderIDParams{ConversationID: conv, ProviderTurnID: provider}
		if indexed {
			return providerTurnSelectConversationTurnByProviderID(ctx, q, a)
		}
		return q.SelectConversationTurnByProviderID(ctx, a)
	case "diff":
		a := gen.UpdateConversationTurnDiffParams{DiffJson: `{"updated":"diff"}`, ConversationID: conv, ProviderTurnID: provider}
		if indexed {
			return providerTurnUpdateConversationTurnDiff(ctx, q, a)
		}
		return q.UpdateConversationTurnDiff(ctx, a)
	case "plan":
		a := gen.UpdateConversationTurnPlanParams{PlanJson: `{"updated":"plan"}`, ConversationID: conv, ProviderTurnID: provider}
		if indexed {
			return providerTurnUpdateConversationTurnPlan(ctx, q, a)
		}
		return q.UpdateConversationTurnPlan(ctx, a)
	}
	panic(name)
}

func TestProviderTurnRoutesPreserveOriginalResultsAndAllFields(t *testing.T) {
	for _, name := range []string{"lookup", "diff", "plan"} {
		for _, conv := range []string{"c", "other", "missing"} {
			for _, provider := range []string{"provider", "", "missing", "failed", "running"} {
				for _, expired := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/%s/expired=%v", name, conv, provider, expired), func(t *testing.T) {
						old, updated := providerTurnFixture(t), providerTurnFixture(t)
						if expired {
							for _, db := range []*sql.DB{old, updated} {
								if _, err := db.Exec("INSERT INTO ao_history_retention VALUES('turn','hit')"); err != nil {
									t.Fatal(err)
								}
							}
						}
						a, ae := runProviderTurnOperation(context.Background(), gen.New(old), name, conv, provider, false)
						b, be := runProviderTurnOperation(context.Background(), gen.New(updated), name, conv, provider, true)
						if fmt.Sprint(ae) != fmt.Sprint(be) || !reflect.DeepEqual(a, b) {
							t.Fatalf("provider-turn result changed: %v/%v vs %v/%v", a, ae, b, be)
						}
						if !reflect.DeepEqual(providerItemRows(t, old, "conversation_turns"), providerItemRows(t, updated, "conversation_turns")) {
							t.Fatal("unrelated/state/expired fields changed")
						}
					})
				}
			}
		}
	}
}

func TestProviderTurnNonemptyRoutesSeekExistingPartialIndex(t *testing.T) {
	db := providerTurnFixture(t)
	for _, name := range []string{"lookup", "diff", "plan"} {
		t.Run(name, func(t *testing.T) {
			capture := &providerItemCapture{DB: db}
			if _, err := runProviderTurnOperation(context.Background(), gen.New(capture), name, "c", "provider", true); err != nil {
				t.Fatal(err)
			}
			rows, err := db.Query("EXPLAIN QUERY PLAN "+capture.query, capture.args...)
			if err != nil {
				t.Fatal(err)
			}
			var plans []string
			for rows.Next() {
				var a, b, c int
				var d string
				if err = rows.Scan(&a, &b, &c, &d); err != nil {
					t.Fatal(err)
				}
				plans = append(plans, d)
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			plan := strings.Join(plans, "\n")
			if !strings.Contains(plan, "idx_conversation_turns_provider (conversation_id=? AND provider_turn_id=?)") || strings.Contains(plan, "idx_conversation_turns_conversation") {
				t.Fatalf("provider turn is not an exact existing-index seek: %s", plan)
			}
			// Empty ID selects the unmodified query, including legacy multi-row updates.
			if _, err := runProviderTurnOperation(context.Background(), gen.New(capture), name, "c", "", true); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(capture.query, "Nonempty") {
				t.Fatal("empty ID used nonempty query")
			}
		})
	}
}

func TestProviderTurnLargeSyntheticLookupCostAndIdentity(t *testing.T) {
	db := providerTurnFixture(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	insert, err := tx.Prepare("INSERT INTO conversation_turns(id,conversation_id,handled_by_session_id,provider_turn_id,state,requested_at,diff_json) VALUES(?,?,'s',?,'completed',?,?)")
	if err != nil {
		t.Fatal(err)
	}
	const total = 12000
	payload := strings.Repeat("synthetic", 128)
	for i := 0; i < total; i++ {
		if _, err = insert.Exec(fmt.Sprintf("scale-%d", i), "scale", fmt.Sprintf("provider-%d", i), time.Date(2026, 10, 1, 0, 0, 0, i, time.UTC), payload); err != nil {
			t.Fatal(err)
		}
	}
	insert.Close()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	q := gen.New(db)
	var originalElapsed, indexedElapsed time.Duration
	for i := 0; i < 64; i++ {
		arg := gen.SelectConversationTurnByProviderIDParams{ConversationID: "scale", ProviderTurnID: fmt.Sprintf("provider-%d", total-1-i)}
		start := time.Now()
		a, ae := q.SelectConversationTurnByProviderID(context.Background(), arg)
		originalElapsed += time.Since(start)
		start = time.Now()
		b, be := providerTurnSelectConversationTurnByProviderID(context.Background(), q, arg)
		indexedElapsed += time.Since(start)
		if ae != nil || be != nil || !reflect.DeepEqual(a, b) {
			t.Fatal("large synthetic point lookup changed full row")
		}
	}
	// Report paired ordinary evidence; correctness does not depend on a noisy
	// elapsed-time threshold or assume a quiet/shared-host performance guarantee.
	t.Logf("synthetic_turns=%d lookups=64 original_seconds=%.6f indexed_seconds=%.6f", total, originalElapsed.Seconds(), indexedElapsed.Seconds())
}

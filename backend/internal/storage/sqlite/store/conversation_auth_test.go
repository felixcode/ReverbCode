package store_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/sqlitetest"
	sqliteDriver "modernc.org/sqlite"
)

func TestPersistedAuthenticationDemandReconcilesOnlyVerifiedCurrentSuccess(t *testing.T) {
	for _, scenario := range []string{"current", "old-generation", "started-before-failure", "child-thread", "missing-authoritative-completion", "newer-failure", "account-change"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			s, err := sqlitetest.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			seedProject(t, s, "auth")
			record := sampleRecord("auth")
			record.Mode = domain.SessionModeChat
			session, err := s.CreateSession(ctx, record)
			if err != nil {
				t.Fatal(err)
			}
			conversation, err := s.CreateConversation(ctx, "auth-conv", domain.ConversationScopeSession, "auth", session.ID, histClock)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.ClaimChatControllerGeneration(ctx, session.ID, "gen-1"); err != nil {
				t.Fatal(err)
			}
			failureAt := histClock.Add(time.Second)
			account := domain.ConversationAccount{ReauthRequiredAt: &failureAt, ReauthReason: "expired", PlanLabel: "Pro"}
			if err := s.RecordAccount(ctx, conversation.ID, account, failureAt); err != nil {
				t.Fatal(err)
			}
			startAt := failureAt.Add(time.Second)
			if scenario == "started-before-failure" {
				startAt = histClock
			}
			if err := s.AdoptProviderTurn(ctx, conversation.ID, session.ID, "gen-1", "turn", "provider-turn", startAt); err != nil {
				t.Fatal(err)
			}
			completedAt := startAt.Add(3 * time.Second)
			nativeID := ""
			if scenario == "child-thread" {
				nativeID = "child"
			}
			if scenario == "missing-authoritative-completion" {
				if err := s.SettleTurn(ctx, conversation.ID, "provider-turn", domain.TurnStateCompleted, "", completedAt); err != nil {
					t.Fatal(err)
				}
			} else {
				payload := fmt.Sprintf(`{"providerTurnId":"provider-turn","providerConversationId":%q,"turnState":"completed"}`, nativeID)
				if _, err := s.ProjectProviderEvent(ctx, conversation.ID, session.ID, "gen-1", "completion", "turn.completed", payload, completedAt, func(txCtx context.Context) error {
					return s.SettleTurn(txCtx, conversation.ID, "provider-turn", domain.TurnStateCompleted, "", completedAt)
				}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "old-generation" {
				if err := s.ClaimChatControllerGeneration(ctx, session.ID, "gen-2"); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "newer-failure" {
				failureAt = completedAt.Add(time.Second)
				account.ReauthRequiredAt = &failureAt
				if err := s.RecordAccount(ctx, conversation.ID, account, failureAt); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "account-change" {
				account.AuthChangedAt = &completedAt
				if err := s.RecordAccount(ctx, conversation.ID, account, completedAt); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = sqlite.OpenPreMigrated(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			// Page loading must reconcile even if the successful turn is outside the page.
			snapshot, err := s.LoadConversationSnapshotPage(ctx, conversation.ID, 0, 1)
			if err != nil {
				t.Fatal(err)
			}
			recovered := snapshot.Conversation.Account.ReauthRequiredAt == nil
			if recovered != (scenario == "current") {
				t.Fatalf("recovered=%v, account=%+v", recovered, snapshot.Conversation.Account)
			}
			if recovered {
				persisted, err := s.ConversationAccount(ctx, conversation.ID)
				if err != nil {
					t.Fatal(err)
				}
				if persisted.AuthenticationState != "authenticated" || persisted.LastAuthFailureReason != "expired" || persisted.PlanLabel != "Pro" {
					t.Fatalf("persisted account = %+v", persisted)
				}
				events, err := s.ProviderEventsSince(ctx, conversation.ID, 0, 10)
				if err != nil || len(events) != 1 {
					t.Fatalf("history lost: %+v, %v", events, err)
				}
			}
		})
	}
}

func TestAccountOnlyChangesInvalidateConversationThroughCDC(t *testing.T) {
	s, session, conversation := conversationFixture(t)
	ctx := context.Background()
	before, err := s.LatestSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	account := domain.ConversationAccount{AuthenticationState: "required", ReauthRequiredAt: &histClock, AuthFailureID: "failure"}
	if err := s.RecordAccount(ctx, conversation, account, histClock); err != nil {
		t.Fatal(err)
	}
	events, err := s.EventsAfter(ctx, before, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("account-only CDC = %+v, %v", events, err)
	}
	if events[0].SessionID != string(session) || string(events[0].Type) != "session_updated" {
		t.Fatalf("account event = %+v", events[0])
	}
	before = events[0].Seq
	if err := s.RecordAccount(ctx, conversation, account, histClock); err != nil {
		t.Fatal(err)
	}
	events, err = s.EventsAfter(ctx, before, 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("unchanged account emitted CDC: %+v, %v", events, err)
	}
	account.AuthenticationState = "authenticated"
	account.ReauthRequiredAt = nil
	account.AuthVerifiedAt = &histClock
	if err := s.RecordAccount(ctx, conversation, account, histClock); err != nil {
		t.Fatal(err)
	}
	events, err = s.EventsAfter(ctx, before, 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("recovery-only CDC = %+v, %v", events, err)
	}
}

func TestAuthenticationRecoveryRollsBackWithProviderProjection(t *testing.T) {
	s, session, conversation := conversationFixture(t)
	ctx := context.Background()
	at := histClock.Add(time.Second)
	account := domain.ConversationAccount{AuthenticationState: "required", ReauthRequiredAt: &at, AuthFailureID: "failure"}
	if err := s.RecordAccount(ctx, conversation, account, at); err != nil {
		t.Fatal(err)
	}
	start := at.Add(time.Second)
	if err := s.AdoptProviderTurn(ctx, conversation, session, "gen-1", "turn", "provider-turn", start); err != nil {
		t.Fatal(err)
	}
	completed := start.Add(time.Second)
	_, err := s.ProjectProviderEvent(ctx, conversation, session, "gen-1", "rolled-back-completion", "turn.completed", `{"providerTurnId":"provider-turn","turnState":"completed"}`, completed, func(txCtx context.Context) error {
		if err := s.SettleTurn(txCtx, conversation, "provider-turn", domain.TurnStateCompleted, "", completed); err != nil {
			return err
		}
		a, err := s.ReconcileConversationAuthentication(txCtx, conversation, "gen-1", "provider-turn", completed)
		if err != nil {
			return err
		}
		if a.ReauthRequiredAt != nil {
			t.Fatal("expected recovery within projection")
		}
		return fmt.Errorf("injected projection failure")
	})
	if err == nil {
		t.Fatal("wanted projection error")
	}
	persisted, err := s.ConversationAccount(ctx, conversation)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.ReauthRequiredAt == nil || persisted.AuthFailureID != "failure" {
		t.Fatalf("rolled back recovery leaked: %+v", persisted)
	}
	events, err := s.ProviderEventsSince(ctx, conversation, 0, 10)
	if err != nil || len(events) != 0 {
		t.Fatalf("rolled back success archived: %+v, %v", events, err)
	}
}

func TestConcurrentRecoveryCannotClearNewerAuthenticationFailure(t *testing.T) {
	s, session, conversation := conversationFixture(t)
	ctx := context.Background()
	failureAt := histClock.Add(time.Second)
	if err := s.RecordAccount(ctx, conversation, domain.ConversationAccount{ReauthRequiredAt: &failureAt}, failureAt); err != nil {
		t.Fatal(err)
	}
	startAt := failureAt.Add(time.Second)
	if err := s.AdoptProviderTurn(ctx, conversation, session, "gen-1", "turn", "provider-turn", startAt); err != nil {
		t.Fatal(err)
	}
	completedAt := startAt.Add(time.Second)
	if _, err := s.ProjectProviderEvent(ctx, conversation, session, "gen-1", "completion", "turn.completed", `{"providerTurnId":"provider-turn","turnState":"completed"}`, completedAt, func(txCtx context.Context) error {
		return s.SettleTurn(txCtx, conversation, "provider-turn", domain.TurnStateCompleted, "", completedAt)
	}); err != nil {
		t.Fatal(err)
	}
	newerFailureAt := completedAt.Add(time.Second)
	ready := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-ready
		results <- s.RecordAccount(ctx, conversation, domain.ConversationAccount{AuthenticationState: "required", ReauthRequiredAt: &newerFailureAt, AuthFailureID: "newer"}, newerFailureAt)
	}()
	go func() {
		<-ready
		_, err := s.ReconcileConversationAuthentication(ctx, conversation, "gen-1", "provider-turn", completedAt)
		results <- err
	}()
	close(ready)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	account, err := s.ConversationAccount(ctx, conversation)
	if err != nil {
		t.Fatal(err)
	}
	if account.ReauthRequiredAt == nil || account.AuthFailureID != "newer" {
		t.Fatalf("old success cleared newer concurrent failure: %+v", account)
	}
}

// Frozen pre-materialization query: the oracle for auth truth and repeated JSON work.
const legacyAuthenticationQuery = `SELECT t.id FROM conversation_turns t
JOIN conversations c ON c.id = t.conversation_id
WHERE t.conversation_id = :conversation_id
  AND t.controller_generation = COALESCE(
    (SELECT controller_generation FROM review WHERE id = c.current_review_id),
    (SELECT controller_generation FROM sessions WHERE id = c.current_session_id))
  AND (:generation = '' OR t.controller_generation = :generation)
  AND t.branch_id = c.active_branch_id
  AND t.state = 'completed' AND t.rolled_back_at IS NULL
  AND t.provider_turn_id <> ''
  AND EXISTS (
    SELECT 1 FROM conversation_provider_events e
    JOIN conversation_branches b ON b.id = c.active_branch_id
    WHERE e.conversation_id = c.id AND e.branch_id = c.active_branch_id
      AND e.method = 'turn.completed'
      AND json_extract(e.payload_json, '$.providerTurnId') = t.provider_turn_id
      AND json_extract(e.payload_json, '$.turnState') = 'completed'
      AND json_extract(e.payload_json, '$.error') IS NULL
      AND COALESCE(json_extract(e.payload_json, '$.providerConversationId'), '') IN ('', b.provider_conversation_id)
  )
  AND (:provider_turn_id = '' OR t.provider_turn_id = :provider_turn_id)
  AND t.started_at > :after_at
  AND t.completed_at > :after_at
ORDER BY t.completed_at DESC LIMIT 1;`

// Use native json_extract for truth, wrapping only its input to count evaluations.
// The probe is unique to these serial tests and safe across go test -count runs.
var authenticationQueryProbe struct {
	once  sync.Once
	calls atomic.Int64
	err   error
}

func authenticationQueryFixture(t *testing.T) (*sql.DB, string, string) {
	t.Helper()
	authenticationQueryProbe.once.Do(func() {
		authenticationQueryProbe.err = sqliteDriver.RegisterScalarFunction("auth_query_test_probe", 1,
			func(_ *sqliteDriver.FunctionContext, args []driver.Value) (driver.Value, error) {
				authenticationQueryProbe.calls.Add(1)
				return args[0], nil
			})
	})
	if authenticationQueryProbe.err != nil {
		t.Fatal(authenticationQueryProbe.err)
	}
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	// Only query columns are needed; index definitions mirror the durable schema.
	_, err = db.Exec(`
CREATE TABLE conversations(id TEXT PRIMARY KEY, active_branch_id TEXT, current_session_id TEXT, current_review_id TEXT);
CREATE TABLE sessions(id TEXT PRIMARY KEY, controller_generation TEXT);
CREATE TABLE review(id TEXT PRIMARY KEY, controller_generation TEXT);
CREATE TABLE conversation_branches(id TEXT PRIMARY KEY, provider_conversation_id TEXT);
CREATE TABLE conversation_provider_events(id INTEGER PRIMARY KEY, conversation_id TEXT, branch_id TEXT, method TEXT, payload_json TEXT);
CREATE INDEX idx_conversation_provider_events_replay ON conversation_provider_events(conversation_id, id);
CREATE INDEX idx_conversation_provider_events_branch ON conversation_provider_events(branch_id, id);
CREATE TABLE conversation_turns(id TEXT PRIMARY KEY, conversation_id TEXT, controller_generation TEXT, branch_id TEXT,
 state TEXT, rolled_back_at INTEGER, provider_turn_id TEXT, started_at INTEGER, completed_at INTEGER, requested_at INTEGER DEFAULT 0);
CREATE INDEX idx_conversation_turns_conversation ON conversation_turns(conversation_id, requested_at);
CREATE INDEX idx_conversation_turns_branch ON conversation_turns(branch_id, requested_at);
CREATE UNIQUE INDEX idx_conversation_turns_provider ON conversation_turns(conversation_id, provider_turn_id) WHERE provider_turn_id <> '';
INSERT INTO conversations VALUES('conversation', 'branch', 'session', NULL);
INSERT INTO sessions VALUES('session', 'generation');
INSERT INTO conversation_branches VALUES('branch', 'root-thread');`)
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile("../queries/conversations.sql")
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	for _, name := range []string{"SelectVerifiedAuthenticationTurn", "SelectVerifiedAuthenticationProviderTurn"} {
		_, query, found := strings.Cut(string(source), "-- name: "+name+" :one\n")
		if !found {
			t.Fatalf("authentication query %s missing", name)
		}
		query, _, _ = strings.Cut(query, "-- name:")
		query = strings.Replace(query, "SELECT t.*", "SELECT t.id", 1)
		for _, arg := range []string{"conversation_id", "generation", "provider_turn_id", "after_at"} {
			query = strings.ReplaceAll(query, "sqlc.arg("+arg+")", ":"+arg)
		}
		queries = append(queries, query)
	}
	return db, queries[0], queries[1]
}

func authenticationQueryResult(t *testing.T, db *sql.DB, query, generation, providerTurn string, cutoff int) string {
	t.Helper()
	var id string
	err := db.QueryRow(query, sql.Named("conversation_id", "conversation"), sql.Named("generation", generation),
		sql.Named("provider_turn_id", providerTurn), sql.Named("after_at", cutoff)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestVerifiedAuthenticationQueryPreservesTruth(t *testing.T) {
	for _, tc := range []struct {
		name, mutation, generation, providerTurn string
		cutoff                                   int
		want                                     string
	}{
		{name: "current", want: "turn"},
		{name: "matching-filters", generation: "generation", providerTurn: "provider-turn", want: "turn"},
		{name: "wrong-generation-filter", generation: "other"},
		{name: "wrong-turn-filter", providerTurn: "other"},
		{name: "wrong-owner-generation", mutation: `UPDATE sessions SET controller_generation='other'`},
		{name: "review-owner", mutation: `INSERT INTO review VALUES('review','generation'); UPDATE conversations SET current_review_id='review'; UPDATE sessions SET controller_generation='other'`, want: "turn"},
		{name: "wrong-review-owner", mutation: `INSERT INTO review VALUES('review','other'); UPDATE conversations SET current_review_id='review'`},
		{name: "missing-owner", mutation: `DELETE FROM sessions`},
		{name: "wrong-turn-generation", mutation: `UPDATE conversation_turns SET controller_generation='other'`},
		{name: "wrong-active-branch", mutation: `UPDATE conversations SET active_branch_id='other'`},
		{name: "wrong-turn-branch", mutation: `UPDATE conversation_turns SET branch_id='other'`},
		{name: "wrong-event-branch", mutation: `UPDATE conversation_provider_events SET branch_id='other'`},
		{name: "wrong-event-conversation", mutation: `UPDATE conversation_provider_events SET conversation_id='other'`},
		{name: "missing-branch", mutation: `DELETE FROM conversation_branches`},
		{name: "rolled-back", mutation: `UPDATE conversation_turns SET rolled_back_at=4`},
		{name: "failed-turn", mutation: `UPDATE conversation_turns SET state='failed'`},
		{name: "recovered-turn", mutation: `UPDATE conversation_turns SET state='recovered'`},
		{name: "empty-provider-turn", mutation: `UPDATE conversation_turns SET provider_turn_id=''`},
		{name: "wrong-event-turn", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.providerTurnId','other')`},
		{name: "missing-event-turn", mutation: `UPDATE conversation_provider_events SET payload_json=json_remove(payload_json,'$.providerTurnId')`},
		{name: "wrong-method", mutation: `UPDATE conversation_provider_events SET method='turn.started'`},
		{name: "missing-proof", mutation: `DELETE FROM conversation_provider_events`},
		{name: "failed-proof", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.turnState','failed')`},
		{name: "missing-proof-state", mutation: `UPDATE conversation_provider_events SET payload_json=json_remove(payload_json,'$.turnState')`},
		{name: "error-proof", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.error','expired')`},
		{name: "empty-error-proof", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.error','')`},
		{name: "object-error-proof", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.error',json('{}'))`},
		{name: "null-error", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.error',NULL)`, want: "turn"},
		{name: "child-thread", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.providerConversationId','child')`},
		{name: "empty-thread", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.providerConversationId','')`, want: "turn"},
		{name: "missing-thread", mutation: `UPDATE conversation_provider_events SET payload_json=json_remove(payload_json,'$.providerConversationId')`, want: "turn"},
		{name: "null-thread", mutation: `UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.providerConversationId',NULL)`, want: "turn"},
		{name: "started-at-cutoff", cutoff: 2},
		{name: "completed-at-cutoff", cutoff: 3},
		{name: "null-start", mutation: `UPDATE conversation_turns SET started_at=NULL`},
		{name: "null-completion", mutation: `UPDATE conversation_turns SET completed_at=NULL`},
		{name: "completion-at-cutoff-only", mutation: `UPDATE conversation_turns SET completed_at=1`, cutoff: 1},
		{name: "duplicate-proof", mutation: `INSERT INTO conversation_provider_events(conversation_id,branch_id,method,payload_json) SELECT conversation_id,branch_id,method,payload_json FROM conversation_provider_events`, want: "turn"},
		{name: "unbound-proof", mutation: `INSERT INTO conversation_provider_events VALUES(2,'conversation','branch','turn.completed','{"providerTurnId":"unbound","turnState":"completed"}')`, want: "turn"},
		{name: "numeric-turn-id", mutation: `UPDATE conversation_turns SET provider_turn_id='123'; UPDATE conversation_provider_events SET payload_json=json_set(payload_json,'$.providerTurnId',123)`, want: "turn"},
		{name: "latest-turn", mutation: `INSERT INTO conversation_turns(id,conversation_id,controller_generation,branch_id,state,rolled_back_at,provider_turn_id,started_at,completed_at) VALUES('latest','conversation','generation','branch','completed',NULL,'latest-provider',4,5); INSERT INTO conversation_provider_events VALUES(2,'conversation','branch','turn.completed','{"providerTurnId":"latest-provider","turnState":"completed"}')`, want: "latest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, query, providerQuery := authenticationQueryFixture(t)
			_, err := db.Exec(`INSERT INTO conversation_turns(id,conversation_id,controller_generation,branch_id,state,rolled_back_at,provider_turn_id,started_at,completed_at) VALUES('turn','conversation','generation','branch','completed',NULL,'provider-turn',2,3);
INSERT INTO conversation_provider_events VALUES(1,'conversation','branch','turn.completed','{"providerTurnId":"provider-turn","turnState":"completed","providerConversationId":"root-thread"}');`)
			if err != nil {
				t.Fatal(err)
			}
			if tc.mutation != "" {
				if _, err := db.Exec(tc.mutation); err != nil {
					t.Fatal(err)
				}
			}
			for name, q := range map[string]string{"legacy": legacyAuthenticationQuery, "proofs": query} {
				if got := authenticationQueryResult(t, db, q, tc.generation, tc.providerTurn, tc.cutoff); got != tc.want {
					t.Fatalf("%s result=%q, want %q", name, got, tc.want)
				}
			}
			providerTurn := tc.providerTurn
			if providerTurn == "" {
				providerTurn = "provider-turn"
				switch tc.name {
				case "latest-turn":
					providerTurn = "latest-provider"
				case "numeric-turn-id":
					providerTurn = "123"
				}
			}
			want := authenticationQueryResult(t, db, legacyAuthenticationQuery, tc.generation, providerTurn, tc.cutoff)
			if got := authenticationQueryResult(t, db, providerQuery, tc.generation, providerTurn, tc.cutoff); got != want {
				t.Fatalf("named provider turn %q result=%q, want %q", providerTurn, got, want)
			}
		})
	}
}

func TestVerifiedAuthenticationQueryScansCompletionProofsOnce(t *testing.T) {
	for _, size := range []int{128, 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db, query, _ := authenticationQueryFixture(t)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			for i := range size {
				id := fmt.Sprintf("turn-%d", i)
				if _, err := tx.Exec(`INSERT INTO conversation_turns(id,conversation_id,controller_generation,branch_id,state,rolled_back_at,provider_turn_id,started_at,completed_at) VALUES(?,'conversation','generation','branch','completed',NULL,?,?,?)`, id, id, i+1, i+2); err != nil {
					t.Fatal(err)
				}
				payload := fmt.Sprintf(`{"providerTurnId":%q,"turnState":"completed"}`, id)
				if _, err := tx.Exec(`INSERT INTO conversation_provider_events(conversation_id,branch_id,method,payload_json) VALUES('conversation','branch','turn.completed',?)`, payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			counts := make(map[string]int64)
			for name, q := range map[string]string{"legacy": legacyAuthenticationQuery, "proofs": query} {
				q = strings.ReplaceAll(q, "json_extract(e.payload_json,", "json_extract(auth_query_test_probe(e.payload_json),")
				authenticationQueryProbe.calls.Store(0)
				if got := authenticationQueryResult(t, db, q, "", "", 0); got != fmt.Sprintf("turn-%d", size-1) {
					t.Fatalf("%s selected %q", name, got)
				}
				counts[name] = authenticationQueryProbe.calls.Load()
			}
			if counts["legacy"] < int64(size*size/2) || counts["proofs"] > int64(4*size) || counts["proofs"] < int64(size) {
				t.Fatalf("JSON evaluations for %d turns: %v", size, counts)
			}
			// Check uninstrumented planner behavior as well as deterministic work counts.
			rows, err := db.Query("EXPLAIN QUERY PLAN "+query, sql.Named("conversation_id", "conversation"),
				sql.Named("generation", ""), sql.Named("provider_turn_id", ""), sql.Named("after_at", 0))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			var isolatedProofs, indexedTurn bool
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				isolatedProofs = isolatedProofs || strings.Contains(detail, "MATERIALIZE completion_proofs") || strings.Contains(detail, "CO-ROUTINE completion_proofs")
				indexedTurn = indexedTurn || strings.Contains(detail, "SEARCH t USING INDEX idx_conversation_turns_provider")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !isolatedProofs || !indexedTurn {
				t.Fatalf("isolated proofs=%v, indexed turn=%v", isolatedProofs, indexedTurn)
			}
			t.Logf("turns=%d legacy_json_calls=%d proof_json_calls=%d", size, counts["legacy"], counts["proofs"])
		})
	}
}

func TestVerifiedAuthenticationProviderTurnQueryBoundsCompletionProofWork(t *testing.T) {
	for _, size := range []int{128, 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			db, query, providerQuery := authenticationQueryFixture(t)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			for i := range size {
				id := fmt.Sprintf("turn-%d", i)
				if _, err := tx.Exec(`INSERT INTO conversation_turns(id,conversation_id,controller_generation,branch_id,state,rolled_back_at,provider_turn_id,started_at,completed_at) VALUES(?,'conversation','generation','branch','completed',NULL,?,?,?)`, id, id, i+1, i+2); err != nil {
					t.Fatal(err)
				}
				payload := fmt.Sprintf(`{"providerTurnId":%q,"turnState":"completed"}`, id)
				if _, err := tx.Exec(`INSERT INTO conversation_provider_events(conversation_id,branch_id,method,payload_json) VALUES('conversation','branch','turn.completed',?)`, payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			providerTurn := fmt.Sprintf("turn-%d", size-1)
			counts := make(map[string]int64)
			for name, q := range map[string]string{"proofs": query, "named": providerQuery} {
				q = strings.ReplaceAll(q, "json_extract(e.payload_json,", "json_extract(auth_query_test_probe(e.payload_json),")
				authenticationQueryProbe.calls.Store(0)
				if got := authenticationQueryResult(t, db, q, "", providerTurn, 0); got != providerTurn {
					t.Fatalf("%s selected %q, want %q", name, got, providerTurn)
				}
				counts[name] = authenticationQueryProbe.calls.Load()
			}
			if counts["named"] > 8 || counts["proofs"] < int64(size) {
				t.Fatalf("JSON evaluations for %d turns: %v", size, counts)
			}
			rows, err := db.Query("EXPLAIN QUERY PLAN "+providerQuery, sql.Named("conversation_id", "conversation"),
				sql.Named("generation", ""), sql.Named("provider_turn_id", providerTurn), sql.Named("after_at", 0))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			var indexedTurn, indexedProof, scannedProof bool
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				indexedTurn = indexedTurn || strings.Contains(detail, "SEARCH t USING INDEX idx_conversation_turns_provider")
				indexedProof = indexedProof || strings.Contains(detail, "SEARCH e USING INDEX idx_conversation_provider_events_branch") || strings.Contains(detail, "SEARCH e USING INDEX idx_conversation_provider_events_replay")
				scannedProof = scannedProof || strings.Contains(detail, "SCAN e")
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !indexedTurn || !indexedProof || scannedProof {
				t.Fatalf("indexed turn=%v, indexed proof=%v, scanned proof=%v", indexedTurn, indexedProof, scannedProof)
			}
			t.Logf("turns=%d proof_json_calls=%d named_json_calls=%d", size, counts["proofs"], counts["named"])
		})
	}
}

package store_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

func explainHTTPQuery(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
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
	return plans
}

func TestRetentionMarkerReadsUseExistingExactIdentityPrimaryKey(t *testing.T) {
	type capturedQuery struct {
		query string
		args  []any
	}
	var captured []capturedQuery
	observer := &retentionObservedDriver{markerQuery: func(q string, args []driver.NamedValue) {
		a := make([]any, len(args))
		for i, v := range args {
			a[i] = v.Value
		}
		captured = append(captured, capturedQuery{q, a})
	}}
	s, writer, _, conversation, _, _ := retentionPoolFixture(t, observer)
	// Keep the competing conversation index in the real schema. EXPLAIN must
	// show page-ID point lookups even with every unrelated audit kind present.
	if _, err := s.LoadConversationSnapshotPage(context.Background(), conversation, 0, 1); err != nil {
		t.Fatal(err)
	}
	if len(captured) == 0 {
		t.Fatal("marker query observer did not run")
	}
	for _, q := range captured {
		plan := strings.Join(explainHTTPQuery(t, writer, q.query, q.args...), "\n")
		if !strings.Contains(plan, "sqlite_autoindex_ao_history_retention_1 (object_kind=? AND object_id=?)") || strings.Contains(plan, "ao_history_retention_conversation") {
			t.Fatalf("page marker lookup scans conversation range: %s", plan)
		}
	}
}

type capturedHTTPDB struct {
	*sql.DB
	query string
	args  []any
}

func (c *capturedHTTPDB) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	c.query = q
	c.args = append([]any(nil), args...)
	return c.DB.QueryContext(ctx, q, args...)
}

const previousCurrentHeadReviewBatchQuery = `-- Batch form of ListCurrentHeadReviewRunsBySession for session-list reads.
-- The latest same-head pass per (session, pr, harness) wins, so the board does
-- not read a superseded retry beside the run that replaced it.
WITH wanted_session AS (
    SELECT CAST(j.value AS TEXT) AS session_id
    FROM json_each(?) AS j
)
SELECT review_run.session_id, review_run.id, review_run.harness, review_run.pr_url, review_run.status, review_run.verdict, review_run.created_at
FROM review_run
JOIN pr ON pr.url = review_run.pr_url
JOIN wanted_session ON wanted_session.session_id = review_run.session_id
WHERE pr.head_sha != ''
  AND review_run.target_sha = pr.head_sha
  AND NOT EXISTS (
      SELECT 1
      FROM review_run newer
      WHERE newer.session_id = review_run.session_id
        AND newer.pr_url = review_run.pr_url
        AND newer.target_sha = review_run.target_sha
        AND newer.harness = review_run.harness
        AND (
            newer.created_at > review_run.created_at
            OR (newer.created_at = review_run.created_at AND newer.id > review_run.id)
        )
  );`

func TestCurrentHeadReviewBatchMatchesOriginalLatestFactsWithoutCorrelatedScan(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		`CREATE TABLE pr(url TEXT PRIMARY KEY,head_sha TEXT)`,
		`CREATE TABLE review_run(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,harness TEXT NOT NULL,pr_url TEXT NOT NULL,target_sha TEXT,status TEXT,verdict TEXT,created_at TIMESTAMP NOT NULL,body TEXT)`,
		`CREATE INDEX idx_review_run_session_batch ON review_run(session_id,created_at) WHERE status='running'`,
		`CREATE UNIQUE INDEX idx_review_run_session_pr_sha_harness ON review_run(session_id,pr_url,target_sha,harness) WHERE target_sha<>'' AND status='running'`,
	} {
		if _, err = db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		url  string
		head any
	}{{"p1", "h1"}, {"p2", "h2"}, {"empty-head", ""}, {"null-head", nil}} {
		if _, err = db.Exec("INSERT INTO pr VALUES(?,?)", row.url, row.head); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Date(2026, 10, 25, 1, 2, 3, 0, time.UTC)
	type fixture struct {
		id, session, harness, pr, target, status, verdict string
		second                                            int
	}
	fixtures := []fixture{
		{"older-completed", "s1", "codex", "p1", "h1", "completed", "approved", 0},
		{"new-completed-a", "s1", "codex", "p1", "h1", "completed", "changes_requested", 1},
		{"new-completed-z", "s1", "codex", "p1", "h1", "completed", "approved", 1},
		{"other-harness", "s1", "claude", "p1", "h1", "completed", "approved", 0},
		{"other-pr", "s1", "codex", "p2", "h2", "failed", "", 0},
		{"old-head", "s1", "codex", "p1", "old", "completed", "approved", 9},
		{"empty-head", "s1", "codex", "empty-head", "", "completed", "approved", 9},
		{"null-head", "s1", "codex", "null-head", "", "completed", "approved", 9},
		{"second-session", "s2", "codex", "p1", "h1", "running", "", 0},
		{"empty-session", "", "codex", "p1", "h1", "completed", "approved", 0},
		{"superseded-running", "s3", "codex", "p1", "h1", "running", "", 0},
		{"later-failed", "s3", "codex", "p1", "h1", "failed", "", 1},
		{"old-failed", "s4", "codex", "p1", "h1", "failed", "", 0},
		{"later-cancelled", "s4", "codex", "p1", "h1", "cancelled", "", 1},
	}
	for _, r := range fixtures {
		if _, err = db.Exec("INSERT INTO review_run VALUES(?,?,?,?,?,?,?,?,?,?)", r.id, r.session, r.harness, r.pr, r.target, r.status, r.verdict, now.Add(time.Duration(r.second)*time.Second), "body must not be selected"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		if _, err = db.Exec("INSERT INTO review_run VALUES(?,?,?,?,?,?,?,?,?,?)", fmt.Sprintf("unrelated-%d", i), "unwanted", "codex", "p1", "h1", "completed", "approved", now, strings.Repeat("private synthetic body ", 100)); err != nil {
			t.Fatal(err)
		}
	}
	sortRows := func(rows []gen.ListCurrentHeadReviewRunsBySessionsRow) {
		sort.Slice(rows, func(i, j int) bool {
			if rows[i].SessionID != rows[j].SessionID {
				return rows[i].SessionID < rows[j].SessionID
			}
			return rows[i].ID < rows[j].ID
		})
	}
	for _, ids := range []string{`[]`, `["s1"]`, `["s1","s1","s2"]`, `["s1",""]`, `[null,"s1"]`, `["missing"]`, `["s1","s2","s3","s4"]`} {
		t.Run(ids, func(t *testing.T) {
			captured := &capturedHTTPDB{DB: db}
			got, err := gen.New(captured).ListCurrentHeadReviewRunsBySessions(context.Background(), ids)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := db.Query(previousCurrentHeadReviewBatchQuery, ids)
			if err != nil {
				t.Fatal(err)
			}
			want := []gen.ListCurrentHeadReviewRunsBySessionsRow{}
			for rows.Next() {
				var r gen.ListCurrentHeadReviewRunsBySessionsRow
				if err = rows.Scan(&r.SessionID, &r.ID, &r.Harness, &r.PRURL, &r.Status, &r.Verdict, &r.CreatedAt); err != nil {
					t.Fatal(err)
				}
				want = append(want, r)
			}
			if err = rows.Err(); err != nil {
				t.Fatal(err)
			}
			rows.Close()
			sortRows(got)
			sortRows(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("latest current-head fact/multiplicity changed: got %+v want %+v", got, want)
			}
			plan := strings.Join(explainHTTPQuery(t, db, captured.query, captured.args...), "\n")
			if strings.Contains(plan, "CORRELATED") || strings.Contains(plan, "SCAN newer") {
				t.Fatalf("correlated rescan remains: %s", plan)
			}
			if strings.Contains(captured.query, "review_run.body") {
				t.Fatal("list lookup reads review body")
			}
		})
	}
}

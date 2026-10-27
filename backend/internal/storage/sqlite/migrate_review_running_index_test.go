package sqlite

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

const reviewRunningQuery = `SELECT 1 FROM review_run WHERE session_id=? AND status='running' LIMIT 1`
const runningIndexName = "idx_review_run_running_session"

func insertIndexReview(db interface {
	Exec(string, ...any) (sql.Result, error)
}, id, owner, target, batch, status string) error {
	_, err := db.Exec(`INSERT INTO review_run(id,review_id,session_id,harness,pr_url,target_sha,batch_id,status,verdict,body,created_at)
 VALUES(?,'fixture-review',?,'codex','fixture-pr',?,?,?,'','synthetic body',CURRENT_TIMESTAMP)`, id, owner, target, batch, status)
	return err
}

func indexReviewRowsSHA(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT rowid,* FROM review_run ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, _ := rows.Columns()
	hash := sha256.New()
	encoder := json.NewEncoder(hash)
	for rows.Next() {
		values, refs := make([]any, len(columns)), make([]any, len(columns))
		for i := range refs {
			refs[i] = &values[i]
		}
		if err = rows.Scan(refs...); err != nil {
			t.Fatal(err)
		}
		if err = encoder.Encode(values); err != nil {
			t.Fatal(err)
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func runningPlan(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`EXPLAIN QUERY PLAN `+reviewRunningQuery, "fixture-owner")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, detail)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(parts, " | ")
}

func TestReviewRunningIndexMigrationPreservesRowsAndUniqueGuard(t *testing.T) {
	db := openMigratedDatabaseCopyNoForeignKeys(t, 191)
	for _, row := range []struct{ id, target, batch, status string }{
		{"empty-a", "", "", "running"}, {"empty-b", "", "", "running"},
		{"batch", "", "batch", "running"}, {"sha", "sha", "", "running"},
		{"both", "other-sha", "batch", "running"}, {"terminal", "sha", "", "complete"},
	} {
		if err := insertIndexReview(db, row.id, "fixture-owner", row.target, row.batch, row.status); err != nil {
			t.Fatal(err)
		}
	}
	before := indexReviewRowsSHA(t, db)
	var original string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='idx_review_run_session_pr_sha_harness'`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	upTo(t, db, 192)
	if before != indexReviewRowsSHA(t, db) {
		t.Fatal("index migration changed review rows or ROWIDs")
	}
	var current, indexSQL string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='idx_review_run_session_pr_sha_harness'`).Scan(&current); err != nil || current != original {
		t.Fatal("existing uniqueness guard changed", err)
	}
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name=?`, runningIndexName).Scan(&indexSQL); err != nil || !strings.Contains(indexSQL, "WHERE status = 'running'") || strings.Contains(indexSQL, "UNIQUE") {
		t.Fatal("running index is not the expected nonunique partial index", err)
	}
	if err := insertIndexReview(db, "duplicate-running-key", "fixture-owner", "sha", "", "running"); err == nil {
		t.Fatal("existing running uniqueness guard was lost")
	}
	downTo(t, db, 191)
	var remaining int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name=?`, runningIndexName).Scan(&remaining); err != nil || remaining != 0 || before != indexReviewRowsSHA(t, db) {
		t.Fatal("index-only rollback changed review facts", err)
	}
}

func TestReviewRunningIndexUsesCoveringLookupAndTracksStatus(t *testing.T) {
	db := openMigratedDatabaseCopyNoForeignKeys(t, 191)
	for i, pair := range [][2]string{{"", ""}, {"sha", ""}, {"", "batch"}, {"other-sha", "batch"}} {
		if err := insertIndexReview(db, fmt.Sprint(i), "fixture-owner", pair[0], pair[1], "running"); err != nil {
			t.Fatal(err)
		}
	}
	if plan := runningPlan(t, db); !strings.Contains(plan, "SCAN review_run") {
		t.Fatal("baseline unexpectedly already covers general running query", plan)
	}
	upTo(t, db, 192)
	plan := runningPlan(t, db)
	if !strings.Contains(plan, "USING COVERING INDEX "+runningIndexName) || strings.Contains(plan, "SCAN review_run") {
		t.Fatal("running query did not use covering partial lookup", plan)
	}
	var version string
	db.QueryRow(`SELECT sqlite_version()`).Scan(&version)
	t.Logf("sqlite_version=%s plan=%s", version, plan)
	check := func(want int) {
		t.Helper()
		var indexed, scanned int
		for _, query := range []string{`SELECT count(*) FROM review_run WHERE session_id=? AND status='running'`, `SELECT count(*) FROM review_run NOT INDEXED WHERE session_id=? AND status='running'`} {
			var count int
			if err := db.QueryRow(query, "fixture-owner").Scan(&count); err != nil || count != want {
				t.Fatal("partial index changed qualification", err)
			}
		}
		indexedErr := db.QueryRow(reviewRunningQuery, "fixture-owner").Scan(&indexed)
		scannedErr := db.QueryRow(`SELECT 1 FROM review_run NOT INDEXED WHERE session_id=? AND status='running' LIMIT 1`, "fixture-owner").Scan(&scanned)
		if (indexedErr == sql.ErrNoRows) != (want == 0) || indexedErr != scannedErr || indexed != scanned {
			t.Fatal("existence result changed")
		}
	}
	check(4)
	for i, terminal := range []string{"complete", "failed", "cancelled", "complete"} {
		if _, err := db.Exec(`UPDATE review_run SET status=? WHERE id=?`, terminal, fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		check(3 - i)
	}
	if _, err := db.Exec(`UPDATE review_run SET status='running' WHERE id='0'`); err != nil {
		t.Fatal(err)
	}
	check(1)
}

func TestReviewRunningIndexSmallSampleDiagnostics(t *testing.T) {
	for _, mode := range []string{"baseline", "partial_running", "full_reference"} {
		t.Run(mode, func(t *testing.T) {
			db := openMigratedDatabaseCopyNoForeignKeys(t, 191)
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 12000; i++ {
				status := "complete"
				if i < 32 {
					status = "running"
				}
				if err = insertIndexReview(tx, fmt.Sprint(i), fmt.Sprintf("owner-%03d", i%1000), "", "", status); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			name := runningIndexName
			build := time.Now()
			if mode == "partial_running" {
				upTo(t, db, 192)
			} else if mode == "full_reference" {
				name = "fixture_review_session_status"
				if _, err = db.Exec(`CREATE INDEX fixture_review_session_status ON review_run(session_id,status)`); err != nil {
					t.Fatal(err)
				}
			}
			buildNS := time.Since(build).Nanoseconds()
			var bytes int64
			if err = db.QueryRow(`SELECT coalesce(sum(pgsize),0) FROM dbstat WHERE name=?`, name).Scan(&bytes); err != nil {
				t.Fatal(err)
			}
			write := time.Now()
			tx, err = db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 1000; i++ {
				if err = insertIndexReview(tx, fmt.Sprintf("new-%d", i), "insert-owner", "", "", "complete"); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			terminalWriteNS := time.Since(write).Nanoseconds()
			write = time.Now()
			if _, err = db.Exec(`UPDATE review_run SET status='complete' WHERE status='running'`); err != nil {
				t.Fatal(err)
			}
			t.Logf("mode=%s fixture_rows=12000 initial_running=32 added_index_bytes=%d build_ns=%d insert_terminal_1000_ns=%d settle_running_32_ns=%d", mode, bytes, buildNS, terminalWriteNS, time.Since(write).Nanoseconds())
		})
	}
}

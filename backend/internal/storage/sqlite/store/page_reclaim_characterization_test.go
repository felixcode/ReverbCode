package store

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type reclaimTestState struct{ Pages, Free, Size int64 }

func reclaimState(t *testing.T, db maintenanceDB) (r reclaimTestState) {
	t.Helper()
	for query, dst := range map[string]*int64{"PRAGMA main.page_count": &r.Pages, "PRAGMA main.freelist_count": &r.Free, "PRAGMA main.page_size": &r.Size} {
		if err := db.QueryRowContext(context.Background(), query).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

// Fixture setup uses DELETE journaling, then selects WAL only for these tiny
// disposable DBs. The production feature must not change journal/auto-vacuum mode.
func reclaimFixture(t *testing.T) (*sql.DB, *sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ao.db")
	w, err := sql.Open("sqlite", path+"?_pragma=auto_vacuum(2)&_pragma=busy_timeout(0)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	w.SetMaxOpenConns(1)
	if _, err = w.Exec(`CREATE TABLE payloads(id INTEGER PRIMARY KEY,body BLOB);CREATE TABLE goose_db_version(version_id INTEGER,is_applied INTEGER);INSERT INTO goose_db_version VALUES(191,1)`); err != nil {
		t.Fatal(err)
	}
	if err = EnsureHistoryRetentionSchema(w); err != nil {
		t.Fatal(err)
	}
	tx, err := w.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		if _, err = tx.Exec(`INSERT INTO payloads VALUES(?,zeroblob(8192))`, i); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Exec(`DELETE FROM payloads WHERE id<127`); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		t.Fatal(err)
	}
	r, err := sql.Open("sqlite", path+"?mode=ro&_pragma=busy_timeout(0)")
	if err != nil {
		t.Fatal(err)
	}
	r.SetMaxOpenConns(1)
	t.Cleanup(func() { r.Close(); w.Close() })
	if s := reclaimState(t, w); s.Free < 16 {
		t.Fatal("insufficient planted free pages", s)
	}
	return w, r, path
}

func drainSingleReclaim(t *testing.T, ctx context.Context, db *sql.Tx) int {
	t.Helper()
	rows, err := db.QueryContext(ctx, `PRAGMA main.incremental_vacuum(1)`)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for rows.Next() {
		n++
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPageReclaimCharacterizationExplicitTransaction(t *testing.T) {
	for _, finish := range []string{"commit", "rollback", "cancel"} {
		t.Run(finish, func(t *testing.T) {
			w, _, _ := reclaimFixture(t)
			before := reclaimState(t, w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tx, err := w.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			resultRows := drainSingleReclaim(t, ctx, tx)
			inside := reclaimState(t, tx)
			if before.Free-inside.Free != 1 || inside.Pages >= before.Pages {
				t.Fatal("one pragma step did not remove exactly one freelist page", before, inside, resultRows)
			}
			switch finish {
			case "commit":
				err = tx.Commit()
			case "rollback":
				err = tx.Rollback()
			case "cancel":
				cancel()
				_ = tx.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}
			after := reclaimState(t, w)
			if finish == "commit" {
				if after.Free != inside.Free || after.Pages != inside.Pages {
					t.Fatal("committed pager state lost", before, inside, after)
				}
			} else if before != after {
				t.Fatal("uncommitted vacuum survived rollback/cancel", before, after)
			}
			t.Logf("mode=%s result_rows=%d free_delta=%d page_delta=%d", finish, resultRows, before.Free-after.Free, before.Pages-after.Pages)
		})
	}
}

func TestPageReclaimCharacterizationWALHeldReader(t *testing.T) {
	w, r, path := reclaimFixture(t)
	reader, err := r.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	var visible int
	if err = reader.QueryRow(`SELECT COUNT(*) FROM payloads`).Scan(&visible); err != nil || visible != 1 {
		t.Fatal("reader snapshot not established", visible, err)
	}
	old := reclaimState(t, reader)
	mainBefore, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := w.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	drainSingleReclaim(t, context.Background(), tx)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	fresh := reclaimState(t, w)
	stillOld := reclaimState(t, reader)
	mainAfter, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Free != old.Free-1 || stillOld != old || mainAfter.Size() != mainBefore.Size() || wal.Size() == 0 {
		t.Fatal("WAL snapshot/main-file distinction missing", old, fresh, stillOld, mainBefore.Size(), mainAfter.Size(), wal.Size())
	}
	t.Logf("logical_pages_removed=%d main_bytes_delta=%d wal_bytes=%d", old.Pages-fresh.Pages, mainAfter.Size()-mainBefore.Size(), wal.Size())
}

func TestPageReclaimCharacterizationAutocheckpointConnectionLocal(t *testing.T) {
	w, _, path := reclaimFixture(t)
	if _, err := w.Exec(`PRAGMA wal_autocheckpoint=1`); err != nil {
		t.Fatal(err)
	}
	second, err := sql.Open("sqlite", path+"?_pragma=wal_autocheckpoint(19)")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var own, other int
	if err = w.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&own); err != nil {
		t.Fatal(err)
	}
	if err = second.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	if own != 1 || other != 19 {
		t.Fatal("autocheckpoint setting escaped its connection", own, other)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Exec(`PRAGMA wal_autocheckpoint=0`); err != nil {
		t.Fatal(err)
	}
	tx, err := w.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	drainSingleReclaim(t, context.Background(), tx)
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Exec(`PRAGMA wal_autocheckpoint=1`); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() {
		t.Fatal("restoring autocheckpoint invoked a checkpoint", before.Size(), after.Size())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var version string
	if err = w.QueryRowContext(ctx, `SELECT sqlite_version()`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("sqlite_version=%s autocheckpoint_original=%d other_connection=%d", version, own, other)
}

package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/url"
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
func reclaimReadonlyDSN(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	return u.String() + "?mode=ro&_pragma=busy_timeout(0)"
}

func requireReclaimReadonly(t *testing.T, c *sql.Conn) {
	t.Helper()
	_, err := c.ExecContext(context.Background(), `UPDATE payloads SET body=body WHERE id=127`)
	var coded interface{ Code() int }
	if !errors.As(err, &coded) || coded.Code()&255 != 8 {
		t.Fatal("connection is not SQLITE_READONLY", err)
	}
	// A real own-main read, fully drained, establishes pager/WAL association.
	var count int
	if err = c.QueryRowContext(context.Background(), `SELECT count(*) FROM main.payloads`).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
}

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
	r, err := sql.Open("sqlite", reclaimReadonlyDSN(path))
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

func TestPageReclaimCharacterizationWriterCloseAndDiscard(t *testing.T) {
	for _, keepIdleReader := range []bool{false, true} {
		for _, discard := range []bool{false, true} {
			name := "last_connection"
			if keepIdleReader {
				name = "idle_readonly_keeper"
			}
			if discard {
				name += "/discard"
			} else {
				name += "/close"
			}
			t.Run(name, func(t *testing.T) {
				w, r, path := reclaimFixture(t)
				if keepIdleReader {
					// Finish the statement: no active read snapshot is held.
					var n int
					if err := r.QueryRow(`SELECT COUNT(*) FROM payloads`).Scan(&n); err != nil || n != 1 {
						t.Fatal(n, err)
					}
				} else if err := r.Close(); err != nil {
					t.Fatal(err)
				}
				connection, err := w.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				tx, err := connection.BeginTx(context.Background(), nil)
				if err != nil {
					t.Fatal(err)
				}
				drainSingleReclaim(t, context.Background(), tx)
				if err = tx.Commit(); err != nil {
					t.Fatal(err)
				}
				before, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if discard {
					if err = connection.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
						t.Fatal("bad connection not discarded", err)
					}
					_ = connection.Close()
				} else {
					if err = connection.Close(); err != nil {
						t.Fatal(err)
					}
					if err = w.Close(); err != nil {
						t.Fatal(err)
					}
				}
				after, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				wal, walErr := os.Stat(path + "-wal")
				walBytes := int64(0)
				if walErr == nil {
					walBytes = wal.Size()
				} else if !os.IsNotExist(walErr) {
					t.Fatal(walErr)
				}
				if keepIdleReader && (before.Size() != after.Size() || walErr != nil) {
					t.Fatal("idle readonly connection did not prevent writer-close checkpoint", before.Size(), after.Size(), walErr)
				}
				if !keepIdleReader && after.Size() >= before.Size() {
					t.Fatal("last close did not exercise expected implicit checkpoint", before.Size(), after.Size(), walBytes)
				}
				t.Logf("idle_keeper=%t discarded=%t main_bytes_delta=%d wal_present=%t wal_bytes=%d", keepIdleReader, discard, after.Size()-before.Size(), walErr == nil, walBytes)
			})
		}
	}
}

// A keeper is temporary: its own last physical close must also leave WAL alone.
// Force MaxIdleConns(0) so Conn.Close cannot merely return a live idle handle.
func TestPageReclaimCharacterizationFinalReadonlyCloseAndReopen(t *testing.T) {
	for _, discardWriter := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "discard"}[discardWriter], func(t *testing.T) {
			w, r, path := reclaimFixture(t)
			r.SetMaxIdleConns(0)
			keeper, err := r.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			requireReclaimReadonly(t, keeper)
			writer, err := w.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			tx, err := writer.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(`UPDATE payloads SET body='committed-wal-sentinel' WHERE id=127`); err != nil {
				t.Fatal(err)
			}
			drainSingleReclaim(t, context.Background(), tx)
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			mainBefore, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			beforeSHA := sha256.Sum256(mainBefore)
			walBefore, err := os.Stat(path + "-wal")
			if err != nil {
				t.Fatal(err)
			}
			if discardWriter {
				if err = writer.Raw(func(any) error { return driver.ErrBadConn }); !errors.Is(err, driver.ErrBadConn) {
					t.Fatal(err)
				}
			}
			_ = writer.Close()
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			if err = keeper.Close(); err != nil {
				t.Fatal(err)
			}
			if err = r.Close(); err != nil {
				t.Fatal(err)
			}
			after, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			walAfter, walErr := os.Stat(path + "-wal")
			walBytes := int64(0)
			if walErr == nil {
				walBytes = walAfter.Size()
			} else if !os.IsNotExist(walErr) {
				t.Fatal(walErr)
			}
			mainAfter, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			afterSHA := sha256.Sum256(mainAfter)
			t.Logf("writer_discarded=%t final_ro_close_main_delta=%d main_hash_equal=%t wal_before=%d wal_after_present=%t wal_after=%d", discardWriter, after.Size()-before.Size(), beforeSHA == afterSHA, walBefore.Size(), walErr == nil, walBytes)
			if before.Size() != after.Size() || beforeSHA != afterSHA {
				t.Fatal("final readonly close changed main bytes")
			}
			reopened, err := sql.Open("sqlite", reclaimReadonlyDSN(path))
			if err != nil {
				t.Fatal(err)
			}
			var body string
			err = reopened.QueryRow(`SELECT CAST(body AS TEXT) FROM payloads WHERE id=127`).Scan(&body)
			closeErr := reopened.Close()
			if err != nil || closeErr != nil || body != "committed-wal-sentinel" {
				t.Fatal("closed WAL not visible after reopen", err, closeErr)
			}
			reopenedMain, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if reopenedMain.Size() != before.Size() {
				t.Fatal("readonly reopen checkpointed")
			}
			t.Logf("writer_discarded=%t final_ro_close_main_delta=%d wal_bytes=%d reopen_committed_value=true", discardWriter, after.Size()-before.Size(), walBytes)
		})
	}
}

func TestPageReclaimCharacterizationURIReadonlyProof(t *testing.T) {
	for _, uri := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain_path", true: "file_uri"}[uri], func(t *testing.T) {
			w, _, path := reclaimFixture(t)
			dsn := path + "?mode=ro&_pragma=busy_timeout(0)"
			if uri {
				dsn = reclaimReadonlyDSN(path)
			}
			pool, err := sql.Open("sqlite", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			conn, err := pool.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_, err = conn.ExecContext(context.Background(), `UPDATE payloads SET body=body WHERE id=127`)
			var coded interface{ Code() int }
			readonly := errors.As(err, &coded) && coded.Code()&255 == 8
			t.Logf("file_uri=%t actual_sqlite_readonly=%t", uri, readonly)
			if uri {
				requireReclaimReadonly(t, conn)
			} else if err != nil {
				t.Fatal("plain DSN behavior changed", err)
			}
			var count int
			if err = w.QueryRow(`SELECT count(*) FROM payloads`).Scan(&count); err != nil || count != 1 {
				t.Fatal(count, err)
			}
		})
	}
}

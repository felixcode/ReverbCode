package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writerContentionFixture(t *testing.T) (*sql.DB, *sql.DB, *sql.DB) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "literal %?# & path")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	dsn := databaseURI(dir) + pragmas
	parsed, err := url.Parse(writerDSN(dsn))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("_txlock") != "immediate" {
		t.Fatal("writer transaction mode absent")
	}
	parsed, err = url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Has("_txlock") {
		t.Fatal("writer mode leaked to reader DSN")
	}
	open := func(s string) *sql.DB {
		db, e := sql.Open("sqlite", s)
		if e != nil {
			t.Fatal(e)
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	external := open(dsn)
	if _, err = external.Exec(`CREATE TABLE owner(generation INTEGER NOT NULL);INSERT INTO owner VALUES(0);CREATE TABLE event(id TEXT PRIMARY KEY,generation INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	return open(writerDSN(dsn)), external, open(dsn)
}
func readGeneration(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) int {
	t.Helper()
	var gen int
	if err := q.QueryRowContext(context.Background(), "SELECT generation FROM owner").Scan(&gen); err != nil {
		t.Fatal(err)
	}
	return gen
}
func expectWriterWaiting(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("second writer crossed existing reservation: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
}

func TestWriterDeferredUpgradeControlAndImmediateReservation(t *testing.T) {
	t.Run("deferred control", func(t *testing.T) {
		_, external, reader := writerContentionFixture(t)
		tx, err := reader.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if readGeneration(t, tx) != 0 {
			t.Fatal("wrong original owner")
		}
		if _, err = external.Exec("UPDATE owner SET generation=1"); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec("INSERT INTO event VALUES('uncommitted',0)")
		var coded interface{ Code() int }
		if !errors.As(err, &coded) || coded.Code()&255 != 5 {
			t.Fatalf("deferred old snapshot did not show BUSY: %v", err)
		}
		t.Logf("synthetic_deferred_upgrade_primary=%d extended=%d", coded.Code()&255, coded.Code())
	})
	t.Run("writer reserves before read", func(t *testing.T) {
		writer, external, reader := writerContentionFixture(t)
		tx, err := writer.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		generation := readGeneration(t, tx)
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() { close(started); _, e := external.Exec("UPDATE owner SET generation=1"); done <- e }()
		<-started
		expectWriterWaiting(t, done)
		// Real archive-identity reads use nil TxOptions; they must remain deferred
		// and readable while another connection holds the WAL write reservation.
		readTx, err := reader.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if readGeneration(t, readTx) != 0 {
			t.Fatal("reader saw uncommitted writer")
		}
		readTx.Rollback()
		if _, err = tx.Exec("INSERT INTO event VALUES('committed',?)", generation); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		select {
		case err = <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(6 * time.Second):
			t.Fatal("external writer never resumed")
		}
		var archived int
		if err = external.QueryRow("SELECT generation FROM event WHERE id='committed'").Scan(&archived); err != nil || archived != 0 {
			t.Fatal("owner/read/archive atomic facts changed")
		}
	})
}

func TestWriterImmediateWaitsBeforeReadingAndWALReaderStaysConcurrent(t *testing.T) {
	for _, commit := range []bool{false, true} {
		t.Run(map[bool]string{false: "external rollback", true: "external commit"}[commit], func(t *testing.T) {
			writer, external, reader := writerContentionFixture(t)
			blocker, err := external.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback()
			if _, err = blocker.Exec("UPDATE owner SET generation=1"); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			started := make(chan struct{})
			seen := make(chan int, 1)
			go func() {
				close(started)
				tx, e := writer.BeginTx(context.Background(), nil)
				if e != nil {
					done <- e
					return
				}
				defer tx.Rollback()
				var g int
				e = tx.QueryRow("SELECT generation FROM owner").Scan(&g)
				if e == nil {
					_, e = tx.Exec("INSERT INTO event VALUES('waited',?)", g)
				}
				if e == nil {
					e = tx.Commit()
				}
				seen <- g
				done <- e
			}()
			<-started
			expectWriterWaiting(t, done)
			ro, err := reader.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			if readGeneration(t, ro) != 0 {
				t.Fatal("WAL reader inherited writer reservation or dirty state")
			}
			ro.Rollback()
			if commit {
				err = blocker.Commit()
			} else {
				err = blocker.Rollback()
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(6 * time.Second):
				t.Fatal("waiting writer did not continue")
			}
			expected := 0
			if commit {
				expected = 1
			}
			if <-seen != expected {
				t.Fatal("read before waiting writer acquired current reservation")
			}
		})
	}
}

func TestWriterCancellationRollsBackAndExactRetryStaysIdempotent(t *testing.T) {
	writer, external, _ := writerContentionFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO event VALUES('same-event',0)"); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = tx.Rollback()
	var n int
	if err = external.QueryRow("SELECT COUNT(*) FROM event").Scan(&n); err != nil || n != 0 {
		t.Fatal("canceled archive partially committed")
	}
	for i := 0; i < 2; i++ {
		tx, err = writer.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		result, e := tx.Exec("INSERT OR IGNORE INTO event VALUES('same-event',0)")
		if e != nil {
			tx.Rollback()
			t.Fatal(e)
		}
		affected, e := result.RowsAffected()
		if e != nil || affected != int64(1-i) {
			tx.Rollback()
			t.Fatal("retry identity changed")
		}
		if e = tx.Commit(); e != nil {
			t.Fatal(e)
		}
	}
	if err = external.QueryRow("SELECT COUNT(*) FROM event").Scan(&n); err != nil || n != 1 {
		t.Fatal("same event retry duplicated")
	}
}

func TestWriterImmediateCanceledWaitLeavesConnectionUsable(t *testing.T) {
	writer, external, _ := writerContentionFixture(t)
	blocker, err := external.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback()
	if _, err = blocker.Exec("UPDATE owner SET generation=1"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		tx, e := writer.BeginTx(ctx, nil)
		if tx != nil {
			tx.Rollback()
		}
		done <- e
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("canceled begin unexpectedly succeeded")
		}
	case <-time.After(6 * time.Second):
		blocker.Rollback()
		<-done
		t.Fatal("canceled lock wait exceeded existing busy bound")
	}
	if err = blocker.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err := writer.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
}

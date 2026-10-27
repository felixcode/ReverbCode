package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	modernsqlite "modernc.org/sqlite"
)

func reclaimRequest(t *testing.T, s *Store) PageReclaimRequest {
	t.Helper()
	cap := s.MaintenanceCapability(context.Background())
	device, ok := cap["dbDevice"].(uint64)
	inode, ok2 := cap["dbInode"].(uint64)
	if !ok || !ok2 || cap["schemaReady"] != true {
		t.Fatal("fixture identity unavailable", cap)
	}
	return PageReclaimRequest{DatabaseDevice: device, DatabaseInode: inode}
}
func mainDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(b)
}

func TestPageReclaimBoundedCommitRestoresPolicyAndPreservesRows(t *testing.T) {
	w, r, path := reclaimFixture(t)
	s := NewStore(w, r)
	req := reclaimRequest(t, s)
	w.Exec(`PRAGMA wal_autocheckpoint=1`)
	w.Exec(`PRAGMA busy_timeout=987`)
	before := reclaimState(t, w)
	beforeDigest := mainDigest(t, path)
	o := DefaultPageReclaimOptions()
	o.MinFreePages = 1
	o.MaxPages = 5
	req.MaxPages = 3
	result, err := s.ReclaimPages(context.Background(), req, o)
	if err != nil || !result.Committed || result.CommitState != "committed" || result.AttemptedSteps != 3 || result.FreelistPagesRemoved != 3 || result.LogicalPagesRemoved < 3 || result.After == nil {
		t.Fatal(result, err)
	}
	after := reclaimState(t, w)
	if before.Free-after.Free != 3 || before.Pages-after.Pages != result.LogicalPagesRemoved || result.LogicalPageBytesRemoved != result.LogicalPagesRemoved*before.Size {
		t.Fatal(before, after, result)
	}
	var busy, auto, count, size int
	w.QueryRow(`PRAGMA busy_timeout`).Scan(&busy)
	w.QueryRow(`PRAGMA wal_autocheckpoint`).Scan(&auto)
	w.QueryRow(`SELECT count(*),length(body) FROM payloads`).Scan(&count, &size)
	if busy != 987 || auto != 1 || count != 1 || size != 8192 || mainDigest(t, path) != beforeDigest || !result.FilesBefore.Known || !result.FilesAfter.Known {
		t.Fatal("settings/data/main bytes changed", busy, auto, count, size, result)
	}
}

func TestPageReclaimUnsupportedModesAndNoWorkHaveNoWrites(t *testing.T) {
	for _, mode := range []int{0, 1, 2} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			w, r, path := reclaimFixtureMode(t, mode)
			s := NewStore(w, r)
			req := reclaimRequest(t, s)
			before := reclaimState(t, w)
			digest := mainDigest(t, path)
			o := DefaultPageReclaimOptions()
			o.MinFreePages = before.Free + 1
			result, err := s.ReclaimPages(context.Background(), req, o)
			want := "unsupported"
			if mode == 2 {
				want = "no_work"
			}
			if err != nil || result.Status != want || result.Committed || result.AttemptedSteps != 0 || result.CommitState != "not_started" {
				t.Fatal(result, err)
			}
			if reclaimState(t, w) != before || mainDigest(t, path) != digest {
				t.Fatal("unsupported/no-work altered DB")
			}
		})
	}
}

func TestPageReclaimIdentityReadOnlyAndLimitsCannotExpand(t *testing.T) {
	for _, kind := range []string{"zero_identity", "other_inode", "invalid_limit", "readonly_store", "request_threshold"} {
		t.Run(kind, func(t *testing.T) {
			w, r, _ := reclaimFixture(t)
			s := NewStore(w, r)
			req := reclaimRequest(t, s)
			o := DefaultPageReclaimOptions()
			o.MinFreePages = 1
			before := reclaimState(t, w)
			switch kind {
			case "zero_identity":
				req.DatabaseInode = 0
			case "other_inode":
				req.DatabaseInode++
			case "invalid_limit":
				req.MaxPages = -1
			case "readonly_store":
				s.readOnly = true
			case "request_threshold":
				req.MinFreePages = before.Free + 1
			}
			result, err := s.ReclaimPages(context.Background(), req, o)
			if kind == "request_threshold" {
				if err != nil || result.Status != "no_work" {
					t.Fatal(result, err)
				}
			} else if err == nil {
				t.Fatal("unsafe request accepted", result)
			}
			if result.Committed || result.AttemptedSteps != 0 || reclaimState(t, w) != before {
				t.Fatal(result)
			}
		})
	}
}

func TestPageReclaimBusinessAndMaintenanceAdmission(t *testing.T) {
	w, r, _ := reclaimFixture(t)
	s := NewStore(w, r)
	req := reclaimRequest(t, s)
	o := DefaultPageReclaimOptions()
	o.MinFreePages = 1
	s.writeMu.Lock()
	result, err := s.ReclaimPages(context.Background(), req, o)
	s.writeMu.Unlock()
	if err != nil || result.Reason != "business_writer_busy" || result.AttemptedSteps != 0 {
		t.Fatal(result, err)
	}
	s.maintenanceBusy.Store(true)
	result, err = s.ReclaimPages(context.Background(), req, o)
	s.maintenanceBusy.Store(false)
	if err != nil || result.Reason != "maintenance_busy" || result.AttemptedSteps != 0 {
		t.Fatal(result, err)
	}
}

// Test-only driver wraps the real modernc connection. It neither substitutes
// SQLite pager behavior nor exposes a driver handle to implementation code.
type reclaimInterceptDriver struct {
	dsn           string
	query         func(string)
	exec          func(string)
	commitUnknown bool
}

func (d *reclaimInterceptDriver) Open(string) (driver.Conn, error) {
	c, e := (&modernsqlite.Driver{}).Open(d.dsn)
	if e != nil {
		return nil, e
	}
	return &reclaimInterceptConn{Conn: c, owner: d}, nil
}

type reclaimInterceptConn struct {
	driver.Conn
	owner *reclaimInterceptDriver
}

func (c *reclaimInterceptConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	return &reclaimInterceptRows{Rows: rows, after: func() {
		if c.owner.query != nil {
			c.owner.query(q)
		}
	}}, nil
}
func (c *reclaimInterceptConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
	if err == nil && c.owner.exec != nil {
		c.owner.exec(q)
	}
	return result, err
}
func (c *reclaimInterceptConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &reclaimInterceptTx{Tx: tx, unknown: c.owner.commitUnknown}, nil
}
func (c *reclaimInterceptConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	var tx driver.Tx
	var err error
	if b, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = b.BeginTx(ctx, o)
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &reclaimInterceptTx{Tx: tx, unknown: c.owner.commitUnknown}, nil
}

type reclaimInterceptTx struct {
	driver.Tx
	unknown bool
}

func (t *reclaimInterceptTx) Commit() error {
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	if t.unknown {
		return errors.New("synthetic_commit_ack_unknown")
	}
	return nil
}

type reclaimInterceptRows struct {
	driver.Rows
	after func()
	once  atomic.Bool
}

func (r *reclaimInterceptRows) Close() error {
	err := r.Rows.Close()
	if !r.once.Swap(true) {
		r.after()
	}
	return err
}

var reclaimDriverSequence atomic.Int64

func interceptReclaimWriter(t *testing.T, s *Store, path string, d *reclaimInterceptDriver) {
	t.Helper()
	name := fmt.Sprintf("reclaim-intercept-%d", reclaimDriverSequence.Add(1))
	d.dsn = "file:" + path + "?_pragma=busy_timeout(0)&_txlock=immediate"
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { db.Close() })
	s.writeDB = db
}

func TestPageReclaimYieldsBetweenStepsAndDoesNotConfirmUnknownCommit(t *testing.T) {
	for _, kind := range []string{"business_waits", "unknown_commit", "cancel_after_step"} {
		t.Run(kind, func(t *testing.T) {
			w, r, path := reclaimFixture(t)
			s := NewStore(w, r)
			req := reclaimRequest(t, s)
			o := DefaultPageReclaimOptions()
			o.MinFreePages = 1
			o.MaxPages = 8
			before := reclaimState(t, w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			d := &reclaimInterceptDriver{commitUnknown: kind == "unknown_commit"}
			d.query = func(q string) {
				if strings.Contains(q, "incremental_vacuum") {
					if kind == "business_waits" {
						s.writeMu.businessWaiting.Store(1)
					}
					if kind == "cancel_after_step" {
						cancel()
					}
				}
			}
			interceptReclaimWriter(t, s, path, d)
			result, err := s.ReclaimPages(ctx, req, o)
			s.writeMu.businessWaiting.Store(0)
			switch kind {
			case "business_waits":
				if err != nil || !result.Committed || result.AttemptedSteps != 1 || result.FreelistPagesRemoved != 1 || result.Reason != "business_writer_waiting" {
					t.Fatal(result, err)
				}
			case "unknown_commit":
				if err == nil || result.Committed || result.CommitState != "unknown" || result.FreelistPagesRemoved != 0 || result.LogicalPagesRemoved != 0 || result.After != nil {
					t.Fatal(result, err)
				}
			case "cancel_after_step":
				if err == nil || result.Committed || result.FreelistPagesRemoved != 0 || result.LogicalPagesRemoved != 0 || result.After != nil || reclaimState(t, w) != before {
					t.Fatal(result, err)
				}
			}
		})
	}
}

func TestPageReclaimAutoCheckpointRestoreFailureDiscardsUnderReadonlyGuard(t *testing.T) {
	w, r, path := reclaimFixture(t)
	s := NewStore(w, r)
	req := reclaimRequest(t, s)
	o := DefaultPageReclaimOptions()
	o.MinFreePages = 1
	// Pending WAL and a different writer connection make a last-close effect real.
	if _, err := w.Exec(`UPDATE payloads SET body='committed-before-discard' WHERE id=127`); err != nil {
		t.Fatal(err)
	}
	before := mainDigest(t, path)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &reclaimInterceptDriver{exec: func(q string) {
		if q == "PRAGMA wal_autocheckpoint=0" {
			cancel()
		}
	}}
	interceptReclaimWriter(t, s, path, d)
	result, err := s.ReclaimPages(ctx, req, o)
	if err == nil || result.Committed || result.CommitState != "not_started" || !result.AutoCheckpointRestoreFailed || !result.ConnectionDiscarded || mainDigest(t, path) != before {
		t.Fatal(result, err)
	}
	var body string
	if err = r.QueryRow(`SELECT CAST(body AS TEXT) FROM payloads WHERE id=127`).Scan(&body); err != nil || body != "committed-before-discard" {
		t.Fatal(err)
	}
}

func TestPageReclaimReadonlyGuardSurvivesCancelAndFinalClose(t *testing.T) {
	w, r, path := reclaimFixture(t)
	s := NewStore(w, r)
	req := reclaimRequest(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	release, err := pinReclaimReadonly(ctx, path, req.DatabaseDevice, req.DatabaseInode)
	if err != nil {
		t.Fatal(err)
	}
	cancel() // Conn acquisition context does not stand for a pinned read snapshot.
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = w.Exec(`UPDATE payloads SET body='guarded-wal-sentinel' WHERE id=127`); err != nil {
		t.Fatal(err)
	}
	before := mainDigest(t, path)
	walBefore, err := os.Stat(path + "-wal")
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if err = release(); err != nil {
		t.Fatal(err)
	}
	walAfter, err := os.Stat(path + "-wal")
	if err != nil || walAfter.Size() != walBefore.Size() || mainDigest(t, path) != before {
		t.Fatal("guard close changed main/WAL", err)
	}
	reopened, err := sql.Open("sqlite", reclaimReadonlyDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var body string
	if err = reopened.QueryRow(`SELECT CAST(body AS TEXT) FROM payloads WHERE id=127`).Scan(&body); err != nil || body != "guarded-wal-sentinel" {
		t.Fatal(err)
	}
}

func TestPageReclaimHeldReaderKeepsPhysicalAndLogicalMeasuresSeparate(t *testing.T) {
	w, r, path := reclaimFixture(t)
	s := NewStore(w, r)
	req := reclaimRequest(t, s)
	o := DefaultPageReclaimOptions()
	o.MinFreePages = 1
	o.MaxPages = 2
	hold, err := sql.Open("sqlite", reclaimReadonlyDSN(path))
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Close()
	tx, err := hold.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRow(`SELECT count(*) FROM payloads`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	result, err := s.ReclaimPages(context.Background(), req, o)
	if err != nil || !result.Committed || result.FreelistPagesRemoved != 2 || result.LogicalPagesRemoved < 2 || !result.FilesAfter.Known || result.FilesBefore.MainBytes != result.FilesAfter.MainBytes || !result.FilesAfter.WALPresent || result.FilesAfter.WALBytes == 0 {
		t.Fatal(result, err)
	}
}

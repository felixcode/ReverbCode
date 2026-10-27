package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"time"
)

type PageReclaimOptions struct {
	MaxPages     int64
	MinFreePages int64
	ReadBudget   time.Duration
	WriteBudget  time.Duration
}

func DefaultPageReclaimOptions() PageReclaimOptions {
	return PageReclaimOptions{MaxPages: 16, MinFreePages: 256, ReadBudget: 2 * time.Second, WriteBudget: 250 * time.Millisecond}
}
func (o PageReclaimOptions) Validate() error {
	if o.MaxPages <= 0 || o.MinFreePages <= 0 || o.ReadBudget <= 0 || o.WriteBudget <= 0 {
		return errors.New("invalid page reclaim settings")
	}
	return nil
}

type PageReclaimRequest struct {
	DatabaseDevice uint64 `json:"database_device"`
	DatabaseInode  uint64 `json:"database_inode"`
	MaxPages       int64  `json:"max_pages,omitempty"`
	MinFreePages   int64  `json:"min_free_pages,omitempty"`
	WriteBudgetNS  int64  `json:"write_budget_ns,omitempty"`
}
type PageReclaimStats struct {
	AutoVacuum    int64 `json:"auto_vacuum"`
	PageSize      int64 `json:"page_size"`
	PageCount     int64 `json:"page_count"`
	FreelistPages int64 `json:"freelist_pages"`
}
type PageReclaimFiles struct {
	Known              bool  `json:"known"`
	MainBytes          int64 `json:"main_bytes"`
	WALBytes           int64 `json:"wal_bytes"`
	MainAllocatedBytes int64 `json:"main_allocated_bytes"`
	WALAllocatedBytes  int64 `json:"wal_allocated_bytes"`
	WALPresent         bool  `json:"wal_present"`
}
type PageReclaimResult struct {
	Status                      string            `json:"status"`
	Reason                      string            `json:"reason,omitempty"`
	CommitState                 string            `json:"commit_state"`
	Committed                   bool              `json:"transaction_committed"`
	AttemptedSteps              int64             `json:"attempted_steps"`
	FreelistPagesRemoved        int64             `json:"freelist_pages_removed"`
	LogicalPagesRemoved         int64             `json:"logical_pages_removed"`
	LogicalPageBytesRemoved     int64             `json:"logical_page_bytes_removed"`
	Before                      *PageReclaimStats `json:"before,omitempty"`
	After                       *PageReclaimStats `json:"after,omitempty"`
	FilesBefore                 PageReclaimFiles  `json:"files_before"`
	FilesAfter                  PageReclaimFiles  `json:"files_after"`
	SQLiteVersion               string            `json:"sqlite_version,omitempty"`
	ConnectionRestoreFailed     bool              `json:"connection_restore_failed"`
	ConnectionDiscarded         bool              `json:"connection_discarded"`
	AutoCheckpointRestoreFailed bool              `json:"autocheckpoint_restore_failed"`
	WriterReservedMS            float64           `json:"writer_reserved_ms"`
	HeldMS                      float64           `json:"held_ms"`
	CommitMS                    float64           `json:"commit_ms"`
	ConnectionReleaseMS         float64           `json:"connection_release_ms"`
	ErrorType                   string            `json:"error_type,omitempty"`
	SQLiteCode                  int               `json:"sqlite_code,omitempty"`
}

func reclaimStats(ctx context.Context, db maintenanceDB) (r PageReclaimStats, err error) {
	for q, dst := range map[string]*int64{"PRAGMA main.auto_vacuum": &r.AutoVacuum, "PRAGMA main.page_size": &r.PageSize, "PRAGMA main.page_count": &r.PageCount, "PRAGMA main.freelist_count": &r.FreelistPages} {
		if err = db.QueryRowContext(ctx, q).Scan(dst); err != nil {
			return r, err
		}
	}
	return r, nil
}

// PageReclaimCapability is a readonly observation, never a mode conversion.
func (s *Store) PageReclaimCapability(ctx context.Context) map[string]any {
	r := map[string]any{"configured": true, "supported": false}
	stats, err := reclaimStats(ctx, s.readDB)
	if err != nil {
		r["reason"] = "metadata_unavailable"
		return r
	}
	r["autoVacuum"] = stats.AutoVacuum
	r["freelistPages"] = stats.FreelistPages
	r["supported"] = stats.AutoVacuum == 2
	if stats.AutoVacuum != 2 {
		r["reason"] = "auto_vacuum_not_incremental"
	}
	return r
}

// ReclaimPages only mutates main through the same AO writer/admission used by
// content maintenance. A step is not a hard physical-I/O bound. Connection close
// can perform SQLite implicit I/O. A temporary true-readonly connection with no
// active snapshot covers writer release/discard; its own close is characterized.
func (s *Store) ReclaimPages(ctx context.Context, req PageReclaimRequest, o PageReclaimOptions) (result PageReclaimResult, err error) {
	result = PageReclaimResult{Status: "deferred", CommitState: "not_started"}
	defer func() {
		if err != nil {
			result.Status = "failed"
			result.ErrorType = "reclaim_failed"
			var coded interface{ Code() int }
			if errors.As(err, &coded) {
				result.SQLiteCode = coded.Code()
			}
			if errors.Is(err, context.Canceled) {
				result.ErrorType = "context_canceled"
			}
			if errors.Is(err, context.DeadlineExceeded) {
				result.ErrorType = "deadline_exceeded"
			}
			if !result.Committed {
				result.FreelistPagesRemoved = 0
				result.LogicalPagesRemoved = 0
				result.LogicalPageBytesRemoved = 0
				result.After = nil
			}
		}
	}()
	if s.readOnly {
		return result, errors.New("read-only reclaim store")
	}
	if err = o.Validate(); err != nil {
		return result, err
	}
	if req.DatabaseDevice == 0 || req.DatabaseInode == 0 || req.MaxPages < 0 || req.MinFreePages < 0 || req.WriteBudgetNS < 0 {
		return result, errors.New("invalid reclaim request")
	}
	if req.MaxPages > 0 {
		o.MaxPages = min(o.MaxPages, req.MaxPages)
	}
	if req.MinFreePages > 0 {
		o.MinFreePages = max(o.MinFreePages, req.MinFreePages)
	}
	if req.WriteBudgetNS > 0 {
		o.WriteBudget = min(o.WriteBudget, time.Duration(req.WriteBudgetNS))
	}
	if !s.maintenanceBusy.CompareAndSwap(false, true) {
		result.Reason = "maintenance_busy"
		return result, nil
	}
	defer s.maintenanceBusy.Store(false)
	readCtx, cancelRead := context.WithTimeout(ctx, o.ReadBudget)
	var path string
	var releaseReadonly func() error
	defer func() {
		if releaseReadonly != nil {
			at := time.Now()
			e := releaseReadonly()
			result.ConnectionReleaseMS += maintenanceMillis(at)
			if err == nil && e != nil {
				err = errors.New("reclaim readonly release failed")
			}
		}
		if result.FilesBefore.Known {
			result.FilesAfter = observeReclaimFiles(path, req.DatabaseDevice, req.DatabaseInode)
		}
	}()
	func() {
		defer cancelRead()
		cap := s.MaintenanceCapability(readCtx)
		if cap["schemaReady"] != true {
			err = errors.New("retention schema unavailable")
			return
		}
		if fmt.Sprint(cap["dbDevice"]) != fmt.Sprint(req.DatabaseDevice) || fmt.Sprint(cap["dbInode"]) != fmt.Sprint(req.DatabaseInode) {
			err = errors.New("reclaim database identity changed")
			return
		}
		var seq int
		var name string
		err = s.readDB.QueryRowContext(readCtx, `PRAGMA database_list`).Scan(&seq, &name, &path)
		if err != nil || name != "main" || path == "" {
			if err == nil {
				err = errors.New("reclaim main database unavailable")
			}
			return
		}
		var before PageReclaimStats
		before, err = reclaimStats(readCtx, s.readDB)
		if err != nil {
			return
		}
		result.Before = &before
		err = s.readDB.QueryRowContext(readCtx, `SELECT sqlite_version()`).Scan(&result.SQLiteVersion)
		if err == nil && before.AutoVacuum == 2 && before.FreelistPages >= o.MinFreePages {
			releaseReadonly, err = pinReclaimReadonly(readCtx, path, req.DatabaseDevice, req.DatabaseInode)
		}
	}()
	if err != nil {
		if errors.Is(readCtx.Err(), context.DeadlineExceeded) {
			err = errors.Join(err, readCtx.Err())
		}
		return result, err
	}
	if result.Before.AutoVacuum != 2 {
		result.Status = "unsupported"
		result.Reason = "auto_vacuum_not_incremental"
		return result, nil
	}
	if result.Before.FreelistPages < o.MinFreePages {
		result.Status = "no_work"
		result.Reason = "below_free_threshold"
		return result, nil
	}
	if !s.writeMu.TryMaintenance() {
		result.Reason = "business_writer_busy"
		return result, nil
	}
	defer s.writeMu.Unlock()
	reserved := time.Now()
	defer func() { result.WriterReservedMS = maintenanceMillis(reserved) }()
	writeCtx, cancel := context.WithTimeout(ctx, o.WriteBudget)
	defer cancel()
	connection, err := s.writeDB.Conn(writeCtx)
	if err != nil {
		return result, err
	}
	result.FilesBefore = observeReclaimFiles(path, req.DatabaseDevice, req.DatabaseInode)
	defer func() {
		at := time.Now()
		_ = connection.Close()
		result.ConnectionReleaseMS += maintenanceMillis(at)
	}()
	if !result.FilesBefore.Known {
		return result, errors.New("reclaim main identity changed before writer")
	}
	base := HistoryMaintenanceResult{Retained: map[string]int{}}
	autoDiscarded := false
	err = withMaintenanceConnection(writeCtx, connection, &base, func() error {
		return withReclaimAutoCheckpoint(writeCtx, connection, &result.AutoCheckpointRestoreFailed, &autoDiscarded, func() error {
			if s.writeMu.BusinessWaiting() {
				result.Reason = "business_writer_waiting"
				return nil
			}
			tx, e := connection.BeginTx(writeCtx, nil)
			if e != nil {
				return e
			}
			held := time.Now()
			defer func() { result.HeldMS = maintenanceMillis(held) }()
			result.CommitState = "open"
			defer func() {
				e := tx.Rollback()
				if result.CommitState == "open" {
					if e == nil {
						result.CommitState = "rolled_back"
					} else {
						result.CommitState = "unknown"
					}
				}
			}()
			before, e := reclaimStats(writeCtx, tx)
			if e != nil {
				return e
			}
			result.Before = &before
			if before.AutoVacuum != 2 {
				result.Status = "unsupported"
				result.Reason = "auto_vacuum_not_incremental"
				return nil
			}
			if before.FreelistPages < o.MinFreePages {
				result.Status = "no_work"
				result.Reason = "below_free_threshold"
				return nil
			}
			limit := min(o.MaxPages, before.FreelistPages)
			for i := int64(0); i < limit; i++ {
				if e = writeCtx.Err(); e != nil {
					return e
				}
				if s.writeMu.BusinessWaiting() {
					result.Reason = "business_writer_waiting"
					break
				}
				rows, e := tx.QueryContext(writeCtx, `PRAGMA main.incremental_vacuum(1)`)
				if e != nil {
					return e
				}
				for rows.Next() {
				}
				e = rows.Err()
				closeErr := rows.Close()
				if e != nil {
					return e
				}
				if closeErr != nil {
					return closeErr
				}
				result.AttemptedSteps++
			}
			if e = writeCtx.Err(); e != nil {
				return e
			}
			if result.AttemptedSteps == 0 {
				return nil
			}
			after, e := reclaimStats(writeCtx, tx)
			if e != nil {
				return e
			}
			freeDelta := before.FreelistPages - after.FreelistPages
			pageDelta := before.PageCount - after.PageCount
			if freeDelta < 0 || freeDelta > result.AttemptedSteps || pageDelta < freeDelta {
				return errors.New("unexpected reclaim pager result")
			}
			result.CommitState = "unknown"
			at := time.Now()
			if e = tx.Commit(); e != nil {
				return e
			}
			result.CommitMS = maintenanceMillis(at)
			result.CommitState = "committed"
			result.Committed = true
			result.After = &after
			result.FreelistPagesRemoved = freeDelta
			result.LogicalPagesRemoved = pageDelta
			result.LogicalPageBytesRemoved = pageDelta * before.PageSize
			result.Status = "ok"
			return nil
		})
	})
	if err != nil && errors.Is(writeCtx.Err(), context.DeadlineExceeded) {
		err = errors.Join(err, writeCtx.Err())
	}
	result.ConnectionRestoreFailed = base.ConnectionRestoreFailed || result.AutoCheckpointRestoreFailed
	result.ConnectionDiscarded = base.ConnectionDiscarded || autoDiscarded
	return result, err
}

// Register restoration before changing this connection-local hook. A spent
// budget cannot lend altered checkpoint policy to the next business writer.
func withReclaimAutoCheckpoint(ctx context.Context, c *sql.Conn, restoreFailed, discarded *bool, run func() error) (err error) {
	var prior int
	if err = c.QueryRowContext(ctx, `PRAGMA wal_autocheckpoint`).Scan(&prior); err != nil {
		return err
	}
	defer func() {
		if _, e := c.ExecContext(ctx, fmt.Sprintf("PRAGMA wal_autocheckpoint=%d", prior)); e != nil {
			*restoreFailed = true
			d := c.Raw(func(any) error { return driver.ErrBadConn })
			*discarded = errors.Is(d, driver.ErrBadConn)
			if err == nil {
				err = errors.New("reclaim checkpoint policy restoration failed")
			}
		}
	}()
	if _, err = c.ExecContext(ctx, `PRAGMA wal_autocheckpoint=0`); err != nil {
		return err
	}
	return run()
}

// Ordinary AO reader pools are RW handles. They cannot provide this guarantee.
// This private connection uses a file: URI mode=ro, reads own-main sqlite_schema
// to open its pager/WAL, drains the statement and holds no read transaction.
func pinReclaimReadonly(ctx context.Context, path string, device, inode uint64) (func() error, error) {
	if !observeReclaimFiles(path, device, inode).Known {
		return nil, errors.New("reclaim main identity unavailable")
	}
	u := url.URL{Scheme: "file", Path: path}
	pool, err := sql.Open("sqlite", u.String()+"?mode=ro&_pragma=busy_timeout(0)")
	if err != nil {
		return nil, err
	}
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	conn, err := pool.Conn(ctx)
	if err != nil {
		_ = pool.Close()
		return nil, err
	}
	release := func() error { return errors.Join(conn.Close(), pool.Close()) }
	var count int64
	if err = conn.QueryRowContext(ctx, `SELECT count(*) FROM main.sqlite_schema`).Scan(&count); err != nil {
		_ = release()
		return nil, err
	}
	if !observeReclaimFiles(path, device, inode).Known {
		_ = release()
		return nil, errors.New("reclaim main identity changed")
	}
	return release, nil
}

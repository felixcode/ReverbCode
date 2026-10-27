//go:build linux

// Package historycopy is an offline rehearsal on an exclusively new private
// copy. It is never registered as an AO daemon, API, CLI or live scheduler.
package historycopy

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/maintenance/rawcopy"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

var stages = []string{"raw", "outputs", "message", "activity", "turn", "session", "builtin:change_log", "builtin:telemetry_event", "builtin:notifications", "builtin:agent_switch_failure_outbox", "builtin:agent_switch_failure_receipts"}

type Options struct {
	Copy        rawcopy.Options
	AsOf        time.Time
	Clean       bool
	Maintenance store.HistoryMaintenanceOptions
	MaxBatches  int
	Progress    func(string, int64)
}
type StageReport struct {
	Stage                     string           `json:"stage"`
	Complete                  bool             `json:"complete"`
	Batches                   int64            `json:"batches"`
	ConfirmedBatches          int64            `json:"confirmed_batches"`
	Scanned                   int64            `json:"scanned"`
	Compacted                 int64            `json:"compacted"`
	Deleted                   int64            `json:"deleted"`
	Audited                   int64            `json:"audited"`
	Markers                   int64            `json:"markers"`
	NetBodyColumnBytesRemoved int64            `json:"net_body_column_bytes_removed"`
	Retained                  map[string]int64 `json:"retained"`
	CommitState               string           `json:"last_commit_state"`
	StopReason                string           `json:"stop_reason,omitempty"`
	SQLiteCode                int              `json:"last_sqlite_code,omitempty"`
}
type Pages struct {
	DatabaseBytes int64 `json:"database_bytes"`
	WALBytes      int64 `json:"wal_bytes"`
	PageSize      int64 `json:"page_size"`
	PageCount     int64 `json:"page_count"`
	FreelistPages int64 `json:"freelist_pages"`
}
type Report struct {
	Protocol         int              `json:"protocol"`
	Status           string           `json:"status"`
	Complete         bool             `json:"complete"`
	CleanRequested   bool             `json:"clean_requested"`
	AsOf             string           `json:"as_of"`
	Cutoff           string           `json:"cutoff"`
	SourceSHA256     string           `json:"source_sha256"`
	RawAuditFold     rawcopy.Report   `json:"raw_audit_fold"`
	Stages           []StageReport    `json:"stages"`
	IdentityBefore   []TableIdentity  `json:"identity_before"`
	IdentityAfter    []TableIdentity  `json:"identity_after"`
	PagesBefore      Pages            `json:"pages_before"`
	PagesAfter       Pages            `json:"pages_after"`
	AllocationBefore AllocationReport `json:"allocation_before"`
	AllocationAfter  AllocationReport `json:"allocation_after"`
	StopReason       string           `json:"stop_reason,omitempty"`
	RebuildPerformed bool             `json:"rebuild_performed"`
}

func validSHA(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func pages(ctx context.Context, db *sql.DB, path string) (p Pages, err error) {
	for name, target := range map[string]*int64{"page_size": &p.PageSize, "page_count": &p.PageCount, "freelist_count": &p.FreelistPages} {
		if err = db.QueryRowContext(ctx, "PRAGMA "+name).Scan(target); err != nil {
			return p, err
		}
	}
	info, e := os.Stat(path)
	if e != nil {
		return p, e
	}
	p.DatabaseBytes = info.Size()
	info, e = os.Stat(path + "-wal")
	if e == nil {
		p.WALBytes = info.Size()
	} else if !os.IsNotExist(e) {
		return p, e
	}
	return p, nil
}
func saveReport(dir string, report Report) error {
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(dir, "history-copy-report.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

// Run never accepts an existing working target. Rawcopy establishes a distinct
// inode and rejects unknown process references before any copy mutation. The
// reviewed executor must additionally isolate its fresh Docker-private volume;
// private PID visibility alone is not an offline/live-data proof.
func Run(parent context.Context, o Options) (report Report, err error) {
	report = Report{Protocol: 1, Status: "failed", CleanRequested: o.Clean, Stages: []StageReport{}}
	if !validSHA(o.Copy.ExpectedSHA256) || o.AsOf.IsZero() || o.AsOf.After(time.Now().UTC()) || o.MaxBatches <= 0 {
		return report, errors.New("invalid_copy_manifest")
	}
	if err = o.Maintenance.Validate(); err != nil {
		return report, err
	}
	report.AsOf = o.AsOf.UTC().Format(time.RFC3339Nano)
	report.Cutoff = o.AsOf.UTC().Add(-o.Maintenance.Retention).Format(time.RFC3339Nano)
	ctx, cancel := context.WithTimeout(parent, o.Copy.Budget)
	defer cancel()
	// The existing fold is copy-only, fully qualified, and precedes content work.
	copyOptions := o.Copy
	copyOptions.Apply = o.Clean
	phase := "snapshot_copy"
	copyOptions.Progress = func(stage string, n int64) {
		phase = stage
		if o.Progress != nil {
			o.Progress(stage, n)
		}
	}
	report.RawAuditFold, err = rawcopy.Run(ctx, copyOptions)
	if err != nil {
		report.StopReason = phase + "_failed"
		if err.Error() == "source_sha256_mismatch" {
			report.StopReason = "source_sha256_mismatch"
		}
		return report, err
	}
	report.SourceSHA256 = report.RawAuditFold.SourceSHA256
	defer func() {
		if err != nil {
			report.Status = "failed"
			if report.StopReason == "" {
				report.StopReason = phase + "_failed"
			}
		}
		if e := saveReport(o.Copy.Directory, report); err == nil && e != nil {
			err = e
		}
	}()
	path := filepath.Join(o.Copy.Directory, "raw-audit-working.sqlite3")
	u := url.URL{Scheme: "file", Path: path}
	writer, err := sql.Open("sqlite", u.String()+"?mode=rw&_pragma=busy_timeout(0)&_pragma=foreign_keys(ON)&_txlock=immediate")
	if err != nil {
		return report, err
	}
	defer writer.Close()
	writer.SetMaxOpenConns(1)
	reader, err := sql.Open("sqlite", u.String()+"?mode=ro&_pragma=busy_timeout(0)&_pragma=foreign_keys(ON)")
	if err != nil {
		return report, err
	}
	defer reader.Close()
	reader.SetMaxOpenConns(1)
	phase = "source_identity_census"
	if o.Progress != nil {
		o.Progress(phase, 0)
	}
	sourceURL := url.URL{Scheme: "file", Path: o.Copy.Source}
	sourceReader, e := sql.Open("sqlite", sourceURL.String()+"?mode=ro&immutable=1")
	if e != nil {
		return report, e
	}
	report.IdentityBefore, err = CensusIdentity(ctx, sourceReader)
	if err != nil {
		sourceReader.Close()
		return report, err
	}
	report.PagesBefore, err = pages(ctx, sourceReader, o.Copy.Source)
	if err != nil {
		sourceReader.Close()
		return report, err
	}
	phase = "source_allocation_census"
	if o.Progress != nil {
		o.Progress(phase, 0)
	}
	report.AllocationBefore, err = CensusAllocation(ctx, sourceReader)
	sourceReader.Close()
	if err != nil {
		return report, err
	}
	if !o.Clean {
		report.Status = "preview"
		report.IdentityAfter = report.IdentityBefore
		report.PagesAfter = report.PagesBefore
		report.AllocationAfter = report.AllocationBefore
		return report, nil
	}
	if !report.RawAuditFold.Complete {
		report.Status = "partial"
		report.StopReason = "raw_audit_fold_budget"
		return report, nil
	}
	s := store.NewStore(writer, reader)
	capability := s.MaintenanceCapability(ctx)
	if capability["schemaReady"] != true {
		return report, errors.New("maintenance_schema_unavailable")
	}
	device, okD := capability["dbDevice"].(uint64)
	inode, okI := capability["dbInode"].(uint64)
	if !okD || !okI {
		return report, errors.New("copy_identity_unknown")
	}
	for _, stage := range stages {
		phase = "maintenance_" + stage
		if o.Progress != nil {
			o.Progress("maintenance_"+stage, 0)
		}
		r := StageReport{Stage: stage, Retained: map[string]int64{}, CommitState: "not_started"}
		cursor := ""
		for batch := 0; batch < o.MaxBatches; batch++ {
			if ctx.Err() != nil {
				r.StopReason = "wall_budget"
				break
			}
			req := store.HistoryMaintenanceRequest{Stage: stage, Cursor: cursor, Cutoff: report.Cutoff, DatabaseDevice: device, DatabaseInode: inode}
			result, e := s.MaintainHistorySnapshotBatch(ctx, req, o.Maintenance, o.AsOf)
			r.Batches++
			r.CommitState = result.CommitState
			r.SQLiteCode = result.SQLiteCode
			// Only confirmed results contribute to mutation counts. Unknown commit
			// stops here; neither the cursor nor confirmed effects are invented.
			if result.Committed {
				r.ConfirmedBatches++
				r.Scanned += int64(result.Scanned)
				r.Compacted += int64(result.Compacted)
				r.Deleted += int64(result.Deleted)
				r.Audited += int64(result.Audited)
				r.Markers += result.Markers
				r.NetBodyColumnBytesRemoved += result.LogicalBytes
				for k, n := range result.Retained {
					r.Retained[k] += int64(n)
				}
			}
			if o.Progress != nil {
				o.Progress("maintenance_"+stage, r.Scanned)
			}
			if e != nil {
				r.StopReason = result.ErrorType
				if r.StopReason == "" {
					r.StopReason = "batch_failed"
				}
				report.Stages = append(report.Stages, r)
				return report, e
			}
			if result.Complete {
				r.Complete = true
				break
			}
			if !result.Committed || result.NextCursor == cursor {
				r.StopReason = "no_confirmed_progress"
				break
			}
			cursor = result.NextCursor
		}
		if !r.Complete && r.StopReason == "" {
			r.StopReason = "batch_budget"
		}
		report.Stages = append(report.Stages, r)
		if !r.Complete {
			report.Status = "partial"
			report.StopReason = "stage_incomplete"
			break
		}
	}
	// Do not spend a new uncapped budget after expiration. A missing post census
	// remains explicit; it is never promoted to a rebuild/retention proof.
	if ctx.Err() != nil {
		report.Status = "partial"
		report.StopReason = "wall_budget"
		return report, nil
	}
	phase = "cleaned_identity_census"
	if o.Progress != nil {
		o.Progress(phase, 0)
	}
	report.IdentityAfter, err = CensusIdentity(ctx, reader)
	if err != nil {
		return report, err
	}
	report.PagesAfter, err = pages(ctx, reader, path)
	if err != nil {
		return report, err
	}
	phase = "cleaned_allocation_census"
	if o.Progress != nil {
		o.Progress(phase, 0)
	}
	report.AllocationAfter, err = CensusAllocation(ctx, reader)
	if err != nil {
		return report, err
	}
	// Last close can checkpoint a private copy's WAL as normal SQLite behavior.
	// Record physical file lengths after that close, without running checkpoint.
	if err = reader.Close(); err != nil {
		return report, err
	}
	if err = writer.Close(); err != nil {
		return report, err
	}
	info, e := os.Stat(path)
	if e != nil {
		return report, e
	}
	report.PagesAfter.DatabaseBytes = info.Size()
	report.PagesAfter.WALBytes = 0
	if wal, e := os.Stat(path + "-wal"); e == nil {
		report.PagesAfter.WALBytes = wal.Size()
	} else if !os.IsNotExist(e) {
		return report, e
	}
	report.Complete = len(report.Stages) == len(stages)
	for _, r := range report.Stages {
		report.Complete = report.Complete && r.Complete
	}
	if report.Complete {
		report.Status = "complete"
	} else if !strings.Contains(report.Status, "partial") {
		report.Status = "partial"
	}
	return report, nil
}

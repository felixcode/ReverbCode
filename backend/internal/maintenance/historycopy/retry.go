//go:build linux

package historycopy

import (
	"context"
	"errors"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// BatchAttempt has safe diagnostics only; object cursors stay in the private
// checkpoint file, never in stdout/progress or this report.
type BatchAttempt struct {
	Rows             int     `json:"requested_rows"`
	Phase            string  `json:"phase"`
	CommitState      string  `json:"commit_state"`
	ErrorType        string  `json:"error_type,omitempty"`
	SQLiteCode       int     `json:"sqlite_code,omitempty"`
	ReadMS           float64 `json:"read_ms"`
	WriterReservedMS float64 `json:"writer_reserved_ms"`
	WaitMS           float64 `json:"wait_ms"`
	HeldMS           float64 `json:"held_ms"`
	CommitMS         float64 `json:"commit_ms"`
}

func describeAttempt(rows int, r store.HistoryMaintenanceResult) BatchAttempt {
	return BatchAttempt{rows, r.Phase, r.CommitState, r.ErrorType, r.SQLiteCode, r.ReadMS, r.WriterReservedMS, r.WaitMS, r.HeldMS, r.CommitMS}
}

type maintenanceBatch func(context.Context, store.HistoryMaintenanceRequest, store.HistoryMaintenanceOptions, time.Time) (store.HistoryMaintenanceResult, error)

// Retry only a deadline with an explicit proof that a write transaction never
// opened. The request cursor/time/identity and every byte/time limit are kept;
// only candidate rows shrink. Open, rolled-back and unknown commits never retry.
func runBatch(ctx context.Context, req store.HistoryMaintenanceRequest, o store.HistoryMaintenanceOptions, asOf time.Time, retries int, call maintenanceBatch, observe func(BatchAttempt)) (store.HistoryMaintenanceResult, error) {
	req.BatchRows = o.BatchRows
	for attempt := 0; ; attempt++ {
		result, err := call(ctx, req, o, asOf)
		observe(describeAttempt(req.BatchRows, result))
		if err == nil || ctx.Err() != nil || !errors.Is(err, context.DeadlineExceeded) || attempt >= retries || req.BatchRows <= 1 || result.Committed || result.CommitState != "not_started" || result.NextCursor != req.Cursor {
			return result, err
		}
		req.BatchRows = max(1, req.BatchRows/2)
	}
}

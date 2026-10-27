//go:build linux

package historycopy

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func TestCopyRetryHalvesOnlyRowsAndKeepsExactCursor(t *testing.T) {
	req := store.HistoryMaintenanceRequest{Stage: "raw", Cursor: "211749", Cutoff: "fixed", DatabaseDevice: 1, DatabaseInode: 2}
	opts := store.DefaultHistoryMaintenanceOptions()
	observed := []BatchAttempt{}
	requests := []store.HistoryMaintenanceRequest{}
	calls := 0
	call := func(ctx context.Context, r store.HistoryMaintenanceRequest, o store.HistoryMaintenanceOptions, at time.Time) (store.HistoryMaintenanceResult, error) {
		calls++
		requests = append(requests, r)
		if o != opts || at != testAsOf || r.Cursor != req.Cursor || r.Cutoff != req.Cutoff || r.DatabaseInode != 2 {
			t.Fatal("proof/limits changed")
		}
		if calls < 3 {
			return store.HistoryMaintenanceResult{Phase: "read_prepare", CommitState: "not_started", NextCursor: r.Cursor, ErrorType: "deadline_exceeded", ReadMS: 5001}, context.DeadlineExceeded
		}
		return store.HistoryMaintenanceResult{Phase: "writer_commit", CommitState: "committed", Committed: true, NextCursor: "211774"}, nil
	}
	got, err := runBatch(context.Background(), req, opts, testAsOf, 8, call, func(a BatchAttempt) { observed = append(observed, a) })
	if err != nil || !got.Committed || calls != 3 || requests[0].BatchRows != 100 || requests[1].BatchRows != 50 || requests[2].BatchRows != 25 {
		t.Fatal(got, err, requests)
	}
	if observed[0].Phase != "read_prepare" || observed[0].ReadMS != 5001 {
		t.Fatal(observed)
	}
}

func TestCopyRetryNeverReplaysOpenUnknownOrChangedCursor(t *testing.T) {
	for _, state := range []string{"open", "unknown", "rolled_back", "committed", "not_started"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			call := func(ctx context.Context, r store.HistoryMaintenanceRequest, o store.HistoryMaintenanceOptions, at time.Time) (store.HistoryMaintenanceResult, error) {
				calls++
				cursor := r.Cursor
				if state == "not_started" {
					cursor = "changed"
				}
				return store.HistoryMaintenanceResult{CommitState: state, NextCursor: cursor}, context.DeadlineExceeded
			}
			_, err := runBatch(context.Background(), store.HistoryMaintenanceRequest{Cursor: "5"}, store.DefaultHistoryMaintenanceOptions(), testAsOf, 8, call, func(BatchAttempt) {})
			if err == nil || calls != 1 {
				t.Fatal(calls, err)
			}
		})
	}
}

func TestCopyRetryBudgetParentAndSingleRowBound(t *testing.T) {
	for _, kind := range []string{"disabled", "cancelled", "one_row", "non_deadline", "exhausted"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			opts := store.DefaultHistoryMaintenanceOptions()
			retries := 8
			want := 1
			switch kind {
			case "disabled":
				retries = 0
			case "cancelled":
				cancel()
			case "one_row":
				opts.BatchRows = 1
			case "exhausted":
				retries = 1
				want = 2
			}
			calls := 0
			call := func(ctx context.Context, r store.HistoryMaintenanceRequest, o store.HistoryMaintenanceOptions, at time.Time) (store.HistoryMaintenanceResult, error) {
				calls++
				err := error(context.DeadlineExceeded)
				if kind == "non_deadline" {
					err = errors.New("sqlite other error")
				}
				return store.HistoryMaintenanceResult{CommitState: "not_started", NextCursor: r.Cursor}, err
			}
			_, _ = runBatch(ctx, store.HistoryMaintenanceRequest{Cursor: "9"}, opts, testAsOf, retries, call, func(BatchAttempt) {})
			if calls != want {
				t.Fatal(calls, want)
			}
		})
	}
}

func TestCopyCheckpointIsPrivateConservativePrefix(t *testing.T) {
	dir := t.TempDir()
	if err := saveCheckpoint(dir, "sealed-sha", "fixed-asof", "fixed-cutoff", "raw", "211749"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "history-copy-checkpoint.json")
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	var got map[string]any
	b, _ := os.ReadFile(path)
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["confirmed_cursor"] != "211749" || got["commit_state"] != "committed" {
		t.Fatal(got)
	}
	// A later checkpoint replaces the file atomically, not append duplicate JSON.
	if err = saveCheckpoint(dir, "sealed-sha", "fixed-asof", "fixed-cutoff", "message", "text-id"); err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile(path)
	json.Unmarshal(b, &got)
	if !reflect.DeepEqual(got["confirmed_cursor"], "text-id") {
		t.Fatal(got)
	}
}

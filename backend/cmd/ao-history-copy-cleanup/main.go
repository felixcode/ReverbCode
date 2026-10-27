//go:build linux

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/maintenance/historycopy"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func main() {
	o := historycopy.Options{Maintenance: store.DefaultHistoryMaintenanceOptions()}
	asOf := ""
	var progressInterval time.Duration
	flag.StringVar(&o.Copy.Source, "snapshot", "", "sealed private snapshot; never live ao.db")
	flag.StringVar(&o.Copy.ExpectedSHA256, "expected-source-sha256", "", "required exact sealed snapshot SHA256")
	flag.StringVar(&o.Copy.Directory, "new-working-directory", "", "new private directory; must not exist")
	flag.StringVar(&asOf, "as-of", "", "required source snapshot UTC RFC3339Nano creation time")
	flag.BoolVar(&o.Clean, "clean-copy", false, "clean only the exclusively new private working copy; default preview")
	flag.DurationVar(&o.Maintenance.Retention, "retention", 47*time.Hour, "historical retention horizon, positive and at most 48h")
	flag.Int64Var(&o.Copy.MaxBytes, "max-copy-bytes", 32<<30, "maximum snapshot file bytes")
	flag.IntVar(&o.Maintenance.BatchRows, "batch-rows", 100, "maximum candidates per application batch")
	flag.IntVar(&o.MaxBatches, "max-batches", 100000, "maximum application batches per stage and raw fold")
	flag.Int64Var(&o.Maintenance.MaxRowBytes, "max-row-bytes", 8<<20, "maximum body bytes per row")
	flag.Int64Var(&o.Maintenance.BatchBytes, "batch-bytes", 16<<20, "maximum body bytes prepared per batch")
	flag.DurationVar(&o.Copy.Budget, "wall-budget", 2*time.Hour, "whole copy, fold, census and cleanup wall budget")
	flag.DurationVar(&o.Maintenance.ReadBudget, "read-budget", 2*time.Second, "application preparation deadline per batch")
	flag.DurationVar(&o.Maintenance.WriteBudget, "write-budget", 250*time.Millisecond, "application write deadline per batch")
	flag.DurationVar(&o.Copy.BatchBudget, "fold-budget", time.Second, "raw duplicate-fold deadline per batch")
	flag.DurationVar(&progressInterval, "progress-interval", 30*time.Second, "minimum repeat progress interval; phase changes are immediate")
	flag.Parse()
	if progressInterval <= 0 {
		fmt.Fprintln(os.Stderr, "invalid_progress_interval")
		os.Exit(2)
	}
	lastStage := ""
	lastProgress := time.Time{}
	o.Progress = func(stage string, n int64) {
		now := time.Now()
		if stage != lastStage || now.Sub(lastProgress) >= progressInterval {
			json.NewEncoder(os.Stderr).Encode(map[string]any{"event": "history_copy_progress", "stage": stage, "count": n, "at": now.UTC().Format(time.RFC3339Nano)})
			lastStage = stage
			lastProgress = now
		}
	}
	var err error
	o.AsOf, err = time.Parse(time.RFC3339Nano, asOf)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid_snapshot_time")
		os.Exit(2)
	}
	o.Copy.Rows = o.Maintenance.BatchRows
	o.Copy.MaxBatches = o.MaxBatches
	o.Copy.MaxRowBytes = o.Maintenance.MaxRowBytes
	o.Copy.BatchBytes = o.Maintenance.BatchBytes
	report, err := historycopy.Run(context.Background(), o)
	if e := json.NewEncoder(os.Stdout).Encode(report); e != nil {
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "history_copy_cleanup_failed")
		os.Exit(1)
	}
}

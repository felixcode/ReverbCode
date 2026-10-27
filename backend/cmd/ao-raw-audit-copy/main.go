//go:build linux

// Separate, copy-only tool: not the AO CLI, public API or production maintenance.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aoagents/agent-orchestrator/backend/internal/maintenance/rawcopy"
	"os"
	"time"
)

func main() {
	o := rawcopy.Options{}
	flag.StringVar(&o.Source, "snapshot", "", "sealed local private snapshot (never ao.db)")
	flag.StringVar(&o.Directory, "new-working-directory", "", "new private local directory, must not exist")
	flag.BoolVar(&o.Apply, "apply", false, "fold proven raw audit duplicates only in the newly created copy; default preview")
	flag.Int64Var(&o.MaxBytes, "max-copy-bytes", 32<<30, "maximum sealed snapshot bytes")
	flag.IntVar(&o.Rows, "batch-rows", 100, "maximum raw audit candidates per transaction")
	flag.IntVar(&o.MaxBatches, "max-batches", 10, "maximum batches per new copy")
	flag.Int64Var(&o.MaxRowBytes, "max-row-bytes", 8<<20, "maximum payload read per row")
	flag.Int64Var(&o.BatchBytes, "batch-bytes", 16<<20, "maximum payload bytes prepared per batch")
	flag.DurationVar(&o.Budget, "wall-budget", time.Minute, "total copy/check/batch wall budget")
	flag.DurationVar(&o.BatchBudget, "batch-budget", 250*time.Millisecond, "per-batch wall budget")
	flag.Parse()
	r, err := rawcopy.Run(context.Background(), o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "raw_audit_copy_failed")
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(r); err != nil {
		os.Exit(1)
	}
}

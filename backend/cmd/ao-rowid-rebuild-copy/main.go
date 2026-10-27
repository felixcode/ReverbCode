//go:build linux

// Copy-only offline tool. Never part of the AO CLI, daemon or public API.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/maintenance/rowidcopy"
)

func main() {
	o := rowidcopy.Options{}
	flag.StringVar(&o.Source, "snapshot", "", "sealed local snapshot, never ao.db")
	flag.StringVar(&o.SourceSHA256, "source-sha256", "", "expected verified snapshot SHA-256")
	flag.StringVar(&o.Directory, "new-candidate-directory", "", "new private local directory; must not exist")
	flag.BoolVar(&o.Apply, "rebuild", false, "rebuild into newly created unpublished candidate; default schema preview")
	flag.Int64Var(&o.MaxBytes, "max-database-bytes", 32<<30, "maximum source and candidate bytes")
	flag.DurationVar(&o.Budget, "wall-budget", time.Hour, "total hashing/copy/validation deadline")
	flag.Parse()
	r, err := rowidcopy.Run(context.Background(), o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rowid_rebuild_copy_failed")
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(r); err != nil {
		os.Exit(1)
	}
}

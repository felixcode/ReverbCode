//go:build linux

// Copy-only offline tool. Never part of the AO CLI, daemon or public API.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/maintenance/rowidcopy"
)

func execute(args []string, stdout, stderr io.Writer) int {
	o := rowidcopy.Options{}
	flags := flag.NewFlagSet("ao-rowid-rebuild-copy", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&o.Source, "snapshot", "", "sealed local snapshot, never ao.db")
	flags.StringVar(&o.SourceSHA256, "source-sha256", "", "expected verified snapshot SHA-256")
	flags.StringVar(&o.Directory, "new-candidate-directory", "", "new private local directory; must not exist")
	flags.BoolVar(&o.Apply, "rebuild", false, "rebuild into newly created unpublished candidate; default schema preview")
	flags.Int64Var(&o.MaxBytes, "max-database-bytes", 32<<30, "maximum source and candidate bytes")
	flags.DurationVar(&o.Budget, "wall-budget", time.Hour, "total hashing/copy/validation deadline")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected_positional_argument")
		return 2
	}
	r, err := rowidcopy.Run(context.Background(), o)
	if err != nil {
		fmt.Fprintln(stderr, "rowid_rebuild_copy_failed")
		return 1
	}
	if err = json.NewEncoder(stdout).Encode(r); err != nil {
		return 1
	}
	return 0
}

func main() {
	os.Exit(execute(os.Args[1:], os.Stdout, os.Stderr))
}

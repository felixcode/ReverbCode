// ao-upgrade-schema performs storage-only migration on an explicit scratch copy.
// It never starts a daemon, reconciler, provider process, controller or prompt.
package main

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func main() {
	dir := flag.String("data-dir", "", "existing isolated copy directory containing ao.db")
	flag.Parse()
	if *dir == "" || !filepath.IsAbs(*dir) || strings.Contains(*dir, "/orchestrator-deploy1/") || strings.Contains(*dir, "/state/") {
		emit("failed", "input_validation", "explicit_isolated_data_directory_required")
		os.Exit(2)
	}
	clean := filepath.Clean(*dir)
	canonical, err := filepath.EvalSymlinks(clean)
	if err != nil || canonical != clean {
		emit("failed", "input_validation", "isolated_directory_must_be_canonical")
		os.Exit(2)
	}
	if _, err := os.Stat(filepath.Join(clean, "ao.db")); err != nil {
		emit("failed", "input_validation", "existing_scratch_database_required")
		os.Exit(2)
	}
	store, err := sqlite.Open(clean)
	if err != nil {
		code := "storage_migration_failed"
		for _, known := range []string{"albanews_0148_migration_collision_shape_unknown", "albanews_0148_branch_trigger_shape_unknown", "albanews_0148_harness_shape_unknown", "albanews_0148_harness_bridge_not_applied"} {
			if strings.Contains(err.Error(), known) {
				code = known
				break
			}
		}
		stage, version := sqlite.UpgradeFailureDetails(err)
		result := map[string]any{"status": "failed", "stage": "storage_migration", "error_code": code}
		if stage != "" {
			result["migration_stage"] = stage
		}
		if version != "" {
			result["migration_version"] = version
		}
		_ = json.NewEncoder(os.Stdout).Encode(result)
		os.Exit(1)
	}
	if err = store.Close(); err != nil {
		emit("failed", "storage_close", "scratch_storage_close_failed")
		os.Exit(1)
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"status": "ready", "daemon_started": false, "provider_started": false})
}

func emit(status, stage, code string) {
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"status": status, "stage": stage, "error_code": code})
}

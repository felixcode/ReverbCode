package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// prepareAlbanews0148Bridge repairs two already-applied version numbers whose
// meanings differ in the reviewed local fork. It never changes goose history.
// It runs before 0193+, which depend on the official 0191/0192 schema.
func prepareAlbanews0148Bridge(db *sql.DB) error {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var ledger int
	if err := conn.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name='goose_db_version'`).Scan(&ledger); err != nil || ledger == 0 {
		return err
	}
	applied := func(version int) (bool, error) {
		var value int
		err := conn.QueryRowContext(ctx, `SELECT coalesce((SELECT is_applied FROM goose_db_version WHERE version_id=? ORDER BY id DESC LIMIT 1),0)`, version).Scan(&value)
		return value == 1, err
	}
	first, err := applied(191)
	if err != nil {
		return err
	}
	second, err := applied(192)
	if err != nil {
		return err
	}
	if !first && !second {
		return nil
	}
	column := func(table, name string) (bool, error) {
		var n int
		err := conn.QueryRowContext(ctx, `SELECT count(*) FROM pragma_table_info(?) WHERE name=? AND type='TEXT' AND "notnull"=1 AND dflt_value=?`, table, name, "''").Scan(&n)
		return n == 1, err
	}
	branch, err := column("sessions", "branch_state")
	if err != nil {
		return err
	}
	var schema string
	if err := conn.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='sessions'`).Scan(&schema); err != nil {
		return err
	}
	checkStart := strings.Index(schema, "CHECK (harness IN (")
	if checkStart < 0 {
		return fmt.Errorf("albanews_0148_harness_shape_unknown")
	}
	checkTail := schema[checkStart:]
	checkEnd := strings.Index(checkTail, "))")
	if checkEnd < 0 {
		return fmt.Errorf("albanews_0148_harness_shape_unknown")
	}
	openhands := strings.Contains(checkTail[:checkEnd], "'openhands'")
	var triggerSQL string
	if err := conn.QueryRowContext(ctx, `SELECT coalesce((SELECT sql FROM sqlite_master WHERE type='trigger' AND name='sessions_cdc_update'),'')`).Scan(&triggerSQL); err != nil {
		return err
	}
	if branch && (!strings.Contains(triggerSQL, "OLD.branch_state") || !strings.Contains(triggerSQL, "NEW.branch_state")) {
		return fmt.Errorf("albanews_0148_branch_trigger_shape_unknown")
	}
	// An ordinary official database has both official changes and no collision.
	if first && second && branch && openhands {
		return nil
	}
	localSession, err := column("sessions", "provider_context")
	if err != nil {
		return err
	}
	localReview, err := column("review", "provider_context")
	if err != nil {
		return err
	}
	var indexSQL string
	err = conn.QueryRowContext(ctx, `SELECT coalesce((SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_review_run_running_session'),'')`).Scan(&indexSQL)
	if err != nil {
		return err
	}
	normalized := strings.ToLower(strings.Join(strings.Fields(indexSQL), " "))
	knownIndex := strings.Contains(normalized, "on review_run (session_id)") && strings.Contains(normalized, "where status = 'running'")
	if !first || !second || !localSession || !localReview || !knownIndex {
		return fmt.Errorf("albanews_0148_migration_collision_shape_unknown")
	}
	if !branch {
		// The complete official Up section is applied in one transaction; its
		// existing ledger row remains untouched, and original rowids survive.
		body, err := migrationsFS.ReadFile("migrations/0191_session_branch_state.sql")
		if err != nil {
			return err
		}
		up, err := migrationUp(body)
		if err != nil {
			return err
		}
		if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
			return err
		}
		if _, err = conn.ExecContext(ctx, up); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			return fmt.Errorf("albanews_0148_branch_bridge: %w", err)
		}
		if _, err = conn.ExecContext(ctx, "COMMIT"); err != nil {
			return err
		}
	}
	if !openhands {
		body, err := migrationsFS.ReadFile("migrations/0192_allow_openhands_harness.sql")
		if err != nil {
			return err
		}
		up, err := migrationUp(body)
		if err != nil {
			return err
		}
		// Match the upstream NO TRANSACTION pragma lifecycle on this exact
		// connection. Even an error must leave writable_schema disabled.
		if _, err = conn.ExecContext(ctx, up); err != nil {
			_, _ = conn.ExecContext(ctx, "PRAGMA writable_schema=RESET")
			return fmt.Errorf("albanews_0148_harness_bridge: %w", err)
		}
		if err = conn.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name='sessions'`).Scan(&schema); err != nil {
			return err
		}
		if !strings.Contains(schema, "'openhands'") {
			return fmt.Errorf("albanews_0148_harness_bridge_not_applied")
		}
	}
	return nil
}

func migrationUp(body []byte) (string, error) {
	parts := strings.SplitN(string(body), "-- +goose Up", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("albanews_0148_migration_up_missing")
	}
	return strings.SplitN(parts[1], "-- +goose Down", 2)[0], nil
}

// The local additions follow official migration completion on fresh databases.
// A legacy database already has these columns/index and requires no rewrite.
func ensureAlbanewsProviderSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"sessions", "review"} {
		var n int
		if err = tx.QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name='provider_context'`, table).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err = tx.Exec(`ALTER TABLE ` + table + ` ADD COLUMN provider_context TEXT NOT NULL DEFAULT ''`); err != nil {
				return err
			}
		}
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_review_run_running_session ON review_run (session_id) WHERE status = 'running'`); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS idx_albanews_switch_failure_gate ON agent_switch_failure_outbox(switch_id) WHERE delivered_at IS NULL AND discarded_at IS NULL AND gate_retained='true'`); err != nil {
		return err
	}
	return tx.Commit()
}

// upgradeMigrationError retains a fixed producer stage without changing the
// underlying Goose/SQLite failure. The isolated helper emits no raw error.
type upgradeMigrationError struct {
	stage string
	err   error
}

func (e *upgradeMigrationError) Error() string { return e.err.Error() }
func (e *upgradeMigrationError) Unwrap() error { return e.err }
func migrationFailure(stage string, err error) error {
	return &upgradeMigrationError{stage: stage, err: err}
}

// UpgradeFailureDetails reports only fixed stages and embedded migration IDs.
// All source SQL, database paths and driver messages remain private to callers.
func UpgradeFailureDetails(err error) (stage, version string) {
	var failure *upgradeMigrationError
	if !errors.As(err, &failure) {
		return "", ""
	}
	switch failure.stage {
	case "bridge", "official_goose", "local_provider_schema":
		stage = failure.stage
	default:
		return "", ""
	}
	entries, readErr := migrationsFS.ReadDir("migrations")
	if readErr != nil {
		return stage, ""
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || !strings.Contains(err.Error(), entry.Name()) {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			continue
		}
		n, parseErr := strconv.ParseInt(prefix, 10, 64)
		if parseErr == nil {
			return stage, strconv.FormatInt(n, 10)
		}
	}
	return stage, ""
}

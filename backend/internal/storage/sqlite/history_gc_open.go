package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	sqlitestore "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// OpenHistoryGCExisting never creates a database, runs migrations or starts AO.
// The operator must hold the maintenance lock with the daemon stopped.
func OpenHistoryGCExisting(ctx context.Context, dataDir string, device, inode uint64, apply bool, offlineCacheMiB ...int) (*Store, error) {
	path := filepath.Join(dataDir, "ao.db")
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	st, ok := before.Sys().(*syscall.Stat_t)
	if !ok || !before.Mode().IsRegular() || before.Mode().Perm() != 0600 || st.Uid != uint32(os.Getuid()) || st.Nlink != 1 || uint64(st.Dev) != device || st.Ino != inode {
		return nil, errors.New("gc_database_identity")
	}
	u := url.URL{Scheme: "file", Path: path}
	ro := u.String() + "?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(0)"
	check, err := sql.Open("sqlite", ro)
	if err != nil {
		return nil, err
	}
	var version, auto int
	err = check.QueryRowContext(ctx, `SELECT max(version_id) FROM goose_db_version WHERE is_applied=1`).Scan(&version)
	if err == nil {
		err = check.QueryRowContext(ctx, `PRAGMA auto_vacuum`).Scan(&auto)
	}
	if err != nil {
		check.Close()
		return nil, err
	}
	if (version != 192 && version != 20261028124744441) || auto != 2 {
		check.Close()
		return nil, errors.New("gc_requires_reviewed_schema_incremental")
	}
	// Opening is metadata-only and never migrates. Reuse the runtime's exact
	// marker/policy validation, then resolve the maintenance/read-protection
	// projections without reading a production row.
	shape, err := sqlitestore.NewReadOnlyStore(ctx, check, check)
	if err != nil || shape.MaintenanceCapability(ctx)["schemaReady"] != true {
		check.Close()
		return nil, errors.New("gc_retention_schema_shape")
	}
	queries := []string{
		`SELECT id,is_terminated,updated_at,prompt FROM sessions LIMIT 0`,
		`SELECT id,scope,session_id,current_session_id FROM conversations LIMIT 0`,
		`SELECT id,conversation_id,session_id,provider_event_id,method,payload_json,received_at,branch_id FROM conversation_provider_events LIMIT 0`,
		`SELECT id,conversation_id,turn_id,kind,status,summary,detail_json,command_output,streamed_text,revision,provider_item_id,request_id FROM conversation_activities LIMIT 0`,
		`SELECT id,conversation_id,turn_id,role,streaming,client_message_id,client_payload_hash FROM conversation_messages LIMIT 0`,
		`SELECT id,conversation_id,handled_by_session_id,provider_turn_id,state,branch_id FROM conversation_turns LIMIT 0`,
		`SELECT id,conversation_id,session_id,provider_conversation_id FROM conversation_branches LIMIT 0`,
		`SELECT session_id,state,target_native_session_ref FROM agent_switches LIMIT 0`,
		`SELECT session_id,status FROM review_run LIMIT 0`,
		`SELECT id,ao_session_id,harness,native_session_id,last_generation_id FROM agent_native_sessions LIMIT 0`,
		`SELECT id,binding_id,usage_source_id,estimated_cost_nanos FROM model_usage_events LIMIT 0`,
	}
	if version == 20261028124744441 {
		queries = append(queries,
			`SELECT branch_state,runtime_released_launch_id,stale_binary_since FROM sessions LIMIT 0`,
			`SELECT session_id,launch,harness,release FROM session_harness_releases LIMIT 0`)
	}
	for _, query := range queries {
		rows, shapeErr := check.QueryContext(ctx, query)
		if shapeErr != nil {
			check.Close()
			return nil, errors.New("gc_maintenance_schema_shape")
		}
		rows.Close()
	}
	check.Close()
	mode := "ro"
	pragma := "&_pragma=query_only(1)"
	if apply {
		mode = "rw"
		pragma = "&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(1)&_pragma=wal_autocheckpoint(1000)"
	}
	if len(offlineCacheMiB) > 1 {
		return nil, errors.New("invalid_offline_cache")
	}
	if len(offlineCacheMiB) == 1 {
		cache := offlineCacheMiB[0]
		if cache < 1 || cache > 1024 {
			return nil, errors.New("invalid_offline_cache")
		}
		if apply {
			pragma += "&_pragma=cache_size(-" + strconv.Itoa(cache*1024) + ")"
		}
	}
	dsn := u.String() + "?mode=" + mode + "&_pragma=busy_timeout(0)" + pragma
	w, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1)
	w.SetMaxIdleConns(1)
	if err = w.PingContext(ctx); err != nil {
		w.Close()
		return nil, err
	}
	r, err := sql.Open("sqlite", ro)
	if err != nil {
		w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(1)
	r.SetMaxIdleConns(1)
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		w.Close()
		r.Close()
		return nil, errors.New("gc_database_replaced")
	}
	if !apply {
		return sqlitestore.NewReadOnlyStore(ctx, w, r)
	}
	return sqlitestore.NewStore(w, r), nil
}

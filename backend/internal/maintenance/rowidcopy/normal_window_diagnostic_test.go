//go:build linux

package rowidcopy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	aosqlite "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

// Diagnostic inputs, not product settings or performance acceptance thresholds.
const normalWindowRows = 1000
const normalWindowYield = 100 * time.Millisecond

type normalWindowInput struct {
	IDs       []int64 `json:"-"`
	Cursor    int64   `json:"-"`
	IDHash    string  `json:"id_sha256"`
	SizeHash  string  `json:"size_sha256"`
	Rows      int    `json:"rows"`
	Payload   int64  `json:"payload_column_bytes"`
	AuditRows int64  `json:"raw_sidecar_rows"`
	AuditText int64  `json:"raw_sidecar_text_bytes"`
}

func normalMetadata(ctx context.Context, path string, cursor int64, limit int) (normalWindowInput, error) {
	out := normalWindowInput{Cursor: cursor}
	if cursor < 0 || limit < 1 || limit > normalWindowRows {
		return out, errors.New("window_metadata_limits")
	}
	u := url.URL{Scheme: "file", Path: path}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro")
	if err != nil {
		return out, errors.New("window_metadata_open")
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT id,octet_length(payload_json) FROM conversation_provider_events WHERE id>? ORDER BY id LIMIT ?`, cursor, limit)
	if err != nil {
		return out, errors.New("window_metadata_query")
	}
	idHash, sizeHash := sha256.New(), sha256.New()
	var encoded [8]byte
	for rows.Next() {
		var id, length int64
		if err = rows.Scan(&id, &length); err != nil || id <= cursor || length < 0 {
			rows.Close()
			return out, errors.New("window_metadata_row")
		}
		out.IDs = append(out.IDs, id)
		out.Payload += length
		binary.BigEndian.PutUint64(encoded[:], uint64(id))
		idHash.Write(encoded[:])
		sizeHash.Write(encoded[:])
		binary.BigEndian.PutUint64(encoded[:], uint64(length))
		sizeHash.Write(encoded[:])
	}
	err = rows.Err()
	rows.Close() // Never pin this observer across an application write.
	if err != nil {
		return out, errors.New("window_metadata_rows")
	}
	out.Rows = len(out.IDs)
	out.IDHash, out.SizeHash = hex.EncodeToString(idHash.Sum(nil)), hex.EncodeToString(sizeHash.Sum(nil))
	if out.Rows != 0 {
		args := []any{"raw"}
		placeholders := make([]string, out.Rows)
		for i, id := range out.IDs {
			args = append(args, strconv.FormatInt(id, 10))
			placeholders[i] = "?"
		}
		// Exact compound-PK probes for this <=1000-row window, not a table census.
		q := `SELECT count(*),coalesce(sum(octet_length(object_kind)+octet_length(object_id)+octet_length(conversation_id)+octet_length(semantic_sha256)+octet_length(prompt_sha256)+octet_length(original_sha256)+octet_length(expired_at)+octet_length(cutoff_at)+octet_length(fields_json)),0) FROM ao_history_retention WHERE object_kind=? AND object_id IN (` + strings.Join(placeholders, ",") + `)`
		if err = db.QueryRowContext(ctx, q, args...).Scan(&out.AuditRows, &out.AuditText); err != nil {
			return out, errors.New("window_audit_metadata")
		}
	}
	return out, nil
}

type normalWindowResult struct {
	Name                  string           `json:"name"`
	BatchRows             int              `json:"batch_rows"`
	Input                 normalWindowInput `json:"input"`
	Status                string           `json:"status"`
	Requests              int              `json:"requests"`
	ConfirmedRequests     int              `json:"confirmed_requests"`
	Scanned               int              `json:"confirmed_scanned"`
	Compacted             int              `json:"confirmed_compacted"`
	Audited               int              `json:"confirmed_audited"`
	Markers               int64            `json:"confirmed_sidecar_insertions"`
	LogicalBytes          int64            `json:"confirmed_logical_bytes_delta"`
	Retained              map[string]int   `json:"confirmed_retained"`
	ErrorType             string           `json:"error_enum,omitempty"`
	Phase                 string           `json:"server_phase,omitempty"`
	CommitState           string           `json:"failed_commit_state,omitempty"`
	Timeouts              int              `json:"timeouts"`
	UnknownRequests       int              `json:"unknown_effect_requests"`
	UnconfirmedScanned    int              `json:"failed_result_scanned_not_counted"`
	ReadMS                float64          `json:"read_ms"`
	HeldMS                float64          `json:"held_ms"`
	ReservedMS            float64          `json:"writer_reserved_ms"`
	MaxHeldMS             float64          `json:"max_held_ms"`
	MaxReservedMS         float64          `json:"max_writer_reserved_ms"`
	CallWallMS            float64          `json:"application_call_wall_ms"`
	ElapsedSeconds        float64          `json:"window_elapsed_seconds_including_yield"`
	RowsPerSecond         float64          `json:"confirmed_rows_per_second"`
	After                 normalWindowInput `json:"after"`
}

type normalBatchCall func(context.Context, store.HistoryMaintenanceRequest, store.HistoryMaintenanceOptions) (store.HistoryMaintenanceResult, error)

func normalPhase(value string) string {
	switch value {
	case "validation", "read_capability", "read_begin", "read_candidates", "read_prepare", "read_commit", "writer_admission", "writer_connection", "writer_setup", "writer_begin", "writer_validate", "writer_apply", "writer_commit":
		return value
	}
	return "unknown"
}

func normalWindow(ctx context.Context, call normalBatchCall, input normalWindowInput, req store.HistoryMaintenanceRequest, options store.HistoryMaintenanceOptions, window time.Duration) (out normalWindowResult) {
	out = normalWindowResult{BatchRows: req.BatchRows, Input: input, Status: "no_input", Retained: map[string]int{}}
	if input.Rows == 0 {
		return out
	}
	if input.Rows != len(input.IDs) || input.Rows > normalWindowRows || (req.BatchRows != 10 && req.BatchRows != 100) || options.ReadBudget != 2*time.Second || options.WriteBudget != 250*time.Millisecond {
		out.Status = "invalid_diagnostic_input"
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	start := time.Now()
	defer func() {
		out.ElapsedSeconds = time.Since(start).Seconds()
		out.RowsPerSecond = float64(out.Scanned) / out.ElapsedSeconds
	}()
	cursor, configuredRows := input.Cursor, req.BatchRows
	for out.Scanned < input.Rows {
		deadline, _ := ctx.Deadline()
		if time.Until(deadline) < options.ReadBudget+options.WriteBudget {
			out.Status = "partial_window_budget"
			return out
		}
		req.Cursor = strconv.FormatInt(cursor, 10)
		req.BatchRows = min(configuredRows, input.Rows-out.Scanned)
		req.ReadBudgetNS, req.WriteBudgetNS = int64(options.ReadBudget), int64(options.WriteBudget)
		// Exactly the real server's per-request parent limit, in addition to its
		// internal separate reader and writer limits. No Snapshot entry point.
		requestCtx, stop := context.WithTimeout(ctx, options.ReadBudget+options.WriteBudget)
		at := time.Now()
		result, err := call(requestCtx, req, options)
		out.CallWallMS += float64(time.Since(at)) / float64(time.Millisecond)
		stop()
		out.Requests++
		for _, metric := range []float64{result.ReadMS, result.HeldMS, result.WriterReservedMS} {
			if math.IsNaN(metric) || math.IsInf(metric, 0) || metric < 0 {
				out.Status = "invalid_result_metrics"
				out.UnknownRequests++
				return out
			}
		}
		out.ReadMS += result.ReadMS
		out.HeldMS += result.HeldMS
		out.ReservedMS += result.WriterReservedMS
		out.MaxHeldMS = max(out.MaxHeldMS, result.HeldMS)
		out.MaxReservedMS = max(out.MaxReservedMS, result.WriterReservedMS)
		if err != nil {
			out.Status, out.ErrorType = "partial_error", "unknown"
			switch result.ErrorType {
			case "deadline_exceeded", "context_canceled", "maintenance_failed":
				out.ErrorType = result.ErrorType
			}
			out.Timeouts += boolInt(errors.Is(err, context.DeadlineExceeded) || result.ErrorType == "deadline_exceeded")
			out.Phase = normalPhase(result.Phase)
			switch result.CommitState {
			case "not_started", "rolled_back", "open", "unknown", "committed":
				out.CommitState = result.CommitState
			default:
				out.CommitState = "unknown"
			}
			out.UnknownRequests += boolInt(out.CommitState != "not_started" && out.CommitState != "rolled_back")
			out.UnconfirmedScanned = max(0, result.Scanned)
			return out // Same for 10/100: no retries or extra window budget.
		}
		if result.Status == "deferred" {
			out.Status = "partial_deferred"
			return out
		}
		if result.Status != "ok" || result.Stage != "raw" || !result.Committed || result.CommitState != "committed" || result.Scanned < 1 || result.Scanned > req.BatchRows || result.Scanned > input.Rows-out.Scanned || result.Compacted < 0 || result.Compacted > result.Scanned || result.Audited < 0 || result.Markers < 0 {
			out.Status = "invalid_result"
			out.UnknownRequests++
			return out
		}
		next, parseErr := strconv.ParseInt(result.NextCursor, 10, 64)
		if parseErr != nil || next != input.IDs[out.Scanned+result.Scanned-1] {
			out.Status = "invalid_cursor"
			out.UnknownRequests++
			return out
		}
		out.ConfirmedRequests++
		out.Scanned += result.Scanned
		out.Compacted += result.Compacted
		out.Audited += result.Audited
		out.Markers += result.Markers
		out.LogicalBytes += result.LogicalBytes
		for reason, amount := range result.Retained {
			// These keys originate in the normal Store, not document content.
			out.Retained[reason] += amount
		}
		cursor = next
		select {
		case <-ctx.Done():
			out.Status = "partial_window_budget"
			return out
		case <-time.After(normalWindowYield):
		}
	}
	out.Status = "window_complete"
	return out
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func normalComparable(a, b normalWindowResult) bool {
	return a.Status == "window_complete" && b.Status == "window_complete" && a.Input.Rows > 0 && a.Input.Rows == b.Input.Rows && a.Input.IDHash == b.Input.IDHash && a.Input.SizeHash == b.Input.SizeHash && a.Compacted > 0 && b.Compacted > 0
}

// Opt-in only behind a fixed private recipe. The original remains RO and never
// becomes an AO data directory. No server, provider, session or model is started.
func TestClosedCandidateNormalMaintenanceWindows(t *testing.T) {
	source := os.Getenv("AO_RECOVERY_CANDIDATE_SOURCE")
	if source == "" {
		t.Skip("private_closed_candidate_recipe_only")
	}
	sha := os.Getenv("AO_RECOVERY_CANDIDATE_SHA256")
	var token []uint64
	if json.Unmarshal([]byte(os.Getenv("AO_RECOVERY_CANDIDATE_TOKEN")), &token) != nil || !validCandidateInput(source, sha, token) {
		t.Fatal("normal_window_source_rejected")
	}
	options := store.DefaultHistoryMaintenanceOptions()
	cutoff := time.Now().UTC().Add(-options.Retention - time.Second).Format(time.RFC3339Nano)
	results := []normalWindowResult{}
	setups := []map[string]any{}
	for _, batch := range []int{10, 100} {
		at := time.Now()
		data, err := copyClosedCandidate(source, sha, "/work", token)
		if err != nil {
			t.Fatal("normal_window_private_copy_failed")
		}
		s, err := aosqlite.Open(data) // 0192 applies only to this new private copy.
		if err != nil {
			t.Fatal("normal_window_private_store_open")
		}
		setups = append(setups, map[string]any{"batch_rows": batch, "copy_and_open_seconds_excluded": time.Since(at).Seconds()})
		path := filepath.Join(data, "ao.db")
		info, err := os.Stat(path)
		if err != nil {
			s.Close()
			t.Fatal("normal_window_copy_stat")
		}
		id := candidateToken(info)
		u := url.URL{Scheme: "file", Path: path}
		meta, err := sql.Open("sqlite", u.String()+"?mode=ro")
		if err != nil {
			s.Close()
			t.Fatal("normal_window_bounds_open")
		}
		var low, high, mode int64
		err = meta.QueryRow(`SELECT min(id),max(id) FROM conversation_provider_events`).Scan(&low, &high)
		if err == nil {
			err = meta.QueryRow(`PRAGMA auto_vacuum`).Scan(&mode)
		}
		meta.Close()
		if err != nil || low < 1 || high < low || mode != 2 {
			s.Close()
			t.Fatal("normal_window_bounds_or_mode")
		}
		windows := []struct { name string; cursor int64 }{{"first", low - 1}, {"body_early", 200000}, {"middle", low + (high-low)/2}}
		for _, window := range windows {
			input, err := normalMetadata(context.Background(), path, window.cursor, normalWindowRows)
			if err != nil {
				s.Close()
				t.Fatal("normal_window_input_metadata")
			}
			req := store.HistoryMaintenanceRequest{Stage: "raw", Cutoff: cutoff, DatabaseDevice: id[0], DatabaseInode: id[1], BatchRows: batch}
			result := normalWindow(context.Background(), s.MaintainHistoryBatch, input, req, options, 45*time.Second)
			result.Name = window.name
			if input.Rows > 0 {
				result.After, err = normalMetadata(context.Background(), path, window.cursor, input.Rows)
				if err != nil || result.After.IDHash != input.IDHash || result.Status == "window_complete" && input.Payload-result.After.Payload != result.LogicalBytes {
					s.Close()
					t.Fatal("normal_window_metadata_or_delta_mismatch")
				}
			}
			results = append(results, result)
		}
		if err = s.Close(); err != nil {
			t.Fatal("normal_window_private_store_close")
		}
	}
	comparable := 0
	for i := 0; i < 3; i++ {
		comparable += boolInt(normalComparable(results[i], results[i+3]))
	}
	value := map[string]any{"status": "normal_window_diagnostic_finished", "source_sha256": sha,
		"normal_entry_point": "Store.MaintainHistoryBatch", "snapshot_entry_used": false,
		"read_budget_ns": int64(options.ReadBudget), "write_budget_ns": int64(options.WriteBudget), "request_parent_budget_ns": int64(options.ReadBudget+options.WriteBudget),
		"yield_ns": int64(normalWindowYield), "case_wall_budget_seconds": 45, "cutoff": cutoff,
		"source_copies": 2, "max_rows_per_trial": 3000, "setup": setups, "windows": results,
		"same_input_complete_body_case_pairs": comparable, "sample_sufficient_for_ordinary_window_comparison": comparable > 0,
		"IPC_and_real_business_concurrency": "U_not_exercised", "full_cycle_48h_or_production_ETA": "U_not_proven", "quiet_performance_acceptance": false,
		"copy_and_migration_excluded_from_window_throughput": true, "cache_state_reset": false, "production_actions": []string{}}
	bytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil || os.WriteFile("/work/normal-maintenance-window-result-safe.json", append(bytes, '\n'), 0600) != nil {
		t.Fatal("normal_window_result_write")
	}
}

func TestNormalWindowKnownAndUnknownFailurePreserveConfirmedPrefix(t *testing.T) {
	for _, state := range []string{"not_started", "unknown", "committed"} {
		t.Run(state, func(t *testing.T) {
			ids := make([]int64, 12)
			for i := range ids { ids[i] = int64(i + 1) }
			input := normalWindowInput{Rows: len(ids), IDs: ids}
			calls := 0
			fn := func(ctx context.Context, req store.HistoryMaintenanceRequest, o store.HistoryMaintenanceOptions) (store.HistoryMaintenanceResult, error) {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 2250*time.Millisecond || req.ReadBudgetNS != int64(2*time.Second) || req.WriteBudgetNS != int64(250*time.Millisecond) {
					t.Fatal("normal_window_request_budget_changed")
				}
				if calls == 1 { return store.HistoryMaintenanceResult{Status: "ok", Stage: "raw", CommitState: "committed", Committed: true, Scanned: 10, Compacted: 10, NextCursor: "10"}, nil }
				return store.HistoryMaintenanceResult{Status: "failed", CommitState: state, Scanned: 2, Phase: "writer_commit", ErrorType: "deadline_exceeded", WriterReservedMS: 300.1}, context.DeadlineExceeded
			}
			out := normalWindow(context.Background(), fn, input, store.HistoryMaintenanceRequest{Stage: "raw", BatchRows: 10}, store.DefaultHistoryMaintenanceOptions(), 5*time.Second)
			if calls != 2 || out.Scanned != 10 || out.Compacted != 10 || out.UnconfirmedScanned != 2 || out.UnknownRequests != boolInt(state != "not_started") || out.Timeouts != 1 || out.MaxReservedMS != 300.1 {
				t.Fatal("normal_window_uncertain_result_was_replayed_or_counted")
			}
		})
	}
}

func TestNormalWindowInvalidResultAndSampleStayUnproven(t *testing.T) {
	for _, kind := range []string{"cursor", "count", "metrics"} {
		t.Run(kind, func(t *testing.T) {
			calls := 0
			fn := func(context.Context, store.HistoryMaintenanceRequest, store.HistoryMaintenanceOptions) (store.HistoryMaintenanceResult, error) {
				calls++
				r := store.HistoryMaintenanceResult{Status: "ok", Stage: "raw", CommitState: "committed", Committed: true, Scanned: 1, NextCursor: "private-not-a-cursor"}
				if kind == "count" { r.Scanned = 1001 }
				if kind == "metrics" { r.HeldMS = math.NaN() }
				return r, nil
			}
			out := normalWindow(context.Background(), fn, normalWindowInput{Rows: 1, IDs: []int64{1}}, store.HistoryMaintenanceRequest{Stage: "raw", BatchRows: 10}, store.DefaultHistoryMaintenanceOptions(), 5*time.Second)
			if calls != 1 || out.Scanned != 0 || out.UnknownRequests != 1 || normalComparable(out, out) { t.Fatal("invalid_window_result_confirmed") }
			if _, err := json.Marshal(out); err != nil { t.Fatal("unsafe_window_metric_serialized") }
		})
	}
	a := normalWindowResult{Status: "window_complete", Input: normalWindowInput{Rows: 1, IDHash: "same", SizeHash: "same"}}
	if normalComparable(a, a) { t.Fatal("no_body_sample_claimed_sufficient") }
	a.Compacted = 1
	b := a; b.Input.SizeHash = "other"
	if normalComparable(a, b) || !normalComparable(a, a) { t.Fatal("different_window_inputs_compared") }
}

func TestNormalWindowUsesCurrentApplicationEligibility(t *testing.T) {
	// This is a newly created synthetic fixture, never a historical input.
	source := nonemptyBootSource(t)
	path := filepath.Join(filepath.Dir(source), "ao.db")
	if os.Rename(source, path) != nil { t.Fatal("normal_fixture_rename") }
	db, err := sql.Open("sqlite", path)
	if err != nil { t.Fatal("normal_fixture_open") }
	defer db.Close()
	old := time.Now().UTC().Add(-72*time.Hour).Format(time.RFC3339Nano)
	if _, err = db.Exec(`UPDATE conversation_turns SET state='completed',completed_at=?;`, old); err != nil { t.Fatal("normal_fixture_turn") }
	for i := 0; i < 12; i++ {
		if _, err = db.Exec(`INSERT INTO conversation_provider_events(conversation_id,session_id,method,payload_json,received_at) VALUES('closed-conv','closed-1','item.completed',?,?)`, `{"text":"`+strings.Repeat("synthetic ", 100)+`"}`, old); err != nil { t.Fatal("normal_fixture_raw") }
	}
	s, err := aosqlite.Open(filepath.Dir(path))
	if err != nil { t.Fatal("normal_fixture_store") }
	defer s.Close()
	info, err := os.Stat(path)
	if err != nil { t.Fatal("normal_fixture_stat") }
	id := candidateToken(info)
	input, err := normalMetadata(context.Background(), path, 0, 12)
	if err != nil || input.Rows != 12 { t.Fatal("normal_fixture_metadata") }
	calls := 0
	call := func(ctx context.Context, req store.HistoryMaintenanceRequest, options store.HistoryMaintenanceOptions) (store.HistoryMaintenanceResult, error) {
		calls++
		if calls == 2 {
			// The next NORMAL request must observe new owner facts; a cached
			// snapshot would wrongly compact these remaining two bodies.
			if _, err := db.Exec(`UPDATE sessions SET is_terminated=0 WHERE id='closed-1'`); err != nil { t.Fatal("normal_fixture_owner_change") }
		}
		return s.MaintainHistoryBatch(ctx, req, options)
	}
	opts := store.DefaultHistoryMaintenanceOptions()
	req := store.HistoryMaintenanceRequest{Stage:"raw", BatchRows:10, DatabaseDevice:id[0], DatabaseInode:id[1], Cutoff:time.Now().UTC().Add(-opts.Retention-time.Second).Format(time.RFC3339Nano)}
	out := normalWindow(context.Background(), call, input, req, opts, 5*time.Second)
	after, err := normalMetadata(context.Background(), path, 0, 12)
	if err != nil || calls != 2 || out.Status != "window_complete" || out.Scanned != 12 || out.Compacted != 10 || out.Retained["raw_turn_unattributed"] != 2 || out.Markers != 0 || input.Payload-after.Payload != out.LogicalBytes || out.LogicalBytes <= 0 {
		t.Fatal("normal_current_eligibility_or_confirmed_body_delta")
	}
}

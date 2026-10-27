package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

// HistoryMaintenanceRequest contains no SQL, predicates, replacement values or
// original content. The daemon alone selects policy, bodies and transformations.
type HistoryMaintenanceRequest struct {
	Stage          string `json:"stage"`
	Cursor         string `json:"cursor"`
	Cutoff         string `json:"cutoff"`
	DatabaseDevice uint64 `json:"database_device"`
	DatabaseInode  uint64 `json:"database_inode"`
	BatchRows      int    `json:"batch_rows,omitempty"`
	BatchBytes     int64  `json:"batch_bytes,omitempty"`
	MaxRowBytes    int64  `json:"max_row_bytes,omitempty"`
	ReadBudgetNS   int64  `json:"read_budget_ns,omitempty"`
	WriteBudgetNS  int64  `json:"write_budget_ns,omitempty"`
}
type HistoryMaintenanceOptions struct {
	Retention   time.Duration
	BatchRows   int
	BatchBytes  int64
	MaxRowBytes int64
	ReadBudget  time.Duration
	WriteBudget time.Duration
}

func DefaultHistoryMaintenanceOptions() HistoryMaintenanceOptions {
	return HistoryMaintenanceOptions{47 * time.Hour, 100, 16 << 20, 8 << 20, 2 * time.Second, 250 * time.Millisecond}
}
func (o HistoryMaintenanceOptions) Validate() error {
	if o.Retention <= 0 || o.Retention > 48*time.Hour || o.BatchRows <= 0 || o.BatchBytes <= 0 || o.MaxRowBytes <= 0 || o.ReadBudget <= 0 || o.WriteBudget <= 0 {
		return errors.New("invalid maintenance settings")
	}
	return nil
}

type HistoryMaintenanceResult struct {
	CommitState             string         `json:"commit_state"`
	WriterReservedMS        float64        `json:"writer_reserved_ms"`
	ConnectionRestoreFailed bool           `json:"connection_restore_failed"`
	ConnectionDiscarded     bool           `json:"connection_discarded"`
	ErrorType               string         `json:"error_type,omitempty"`
	SQLiteCode              int            `json:"sqlite_code,omitempty"`
	Status                  string         `json:"status"`
	Stage                   string         `json:"stage"`
	NextCursor              string         `json:"next_cursor"`
	Complete                bool           `json:"complete"`
	Scanned                 int            `json:"scanned"`
	Compacted               int            `json:"compacted"`
	Deleted                 int            `json:"deleted"`
	Audited                 int            `json:"audited"`
	Markers                 int64          `json:"markers"`
	LogicalBytes            int64          `json:"logical_bytes_delta"`
	Retained                map[string]int `json:"retained"`
	Committed               bool           `json:"transaction_committed"`
	ReadMS                  float64        `json:"read_ms"`
	WaitMS                  float64        `json:"wait_ms"`
	HeldMS                  float64        `json:"held_ms"`
	CommitMS                float64        `json:"commit_ms"`
}
type maintenanceStage struct {
	table, kind string
	fields      []string
	builtin     bool
}

var maintenanceStages = map[string]maintenanceStage{
	"raw":                                   {"conversation_provider_events", "provider_event", []string{"payload_json"}, false},
	"outputs":                               {"conversation_activities", "activity_output", []string{"command_output", "streamed_text"}, false},
	"message":                               {"conversation_messages", "message", []string{"text", "delivery_content_json"}, false},
	"activity":                              {"conversation_activities", "activity", []string{"summary", "detail_json", "command_output", "streamed_text"}, false},
	"turn":                                  {"conversation_turns", "turn", []string{"diff_json", "plan_json", "error_message"}, false},
	"session":                               {"sessions", "session", []string{"prompt"}, false},
	"builtin:change_log":                    {"change_log", "", nil, true},
	"builtin:telemetry_event":               {"telemetry_event", "", nil, true},
	"builtin:notifications":                 {"notifications", "", nil, true},
	"builtin:agent_switch_failure_outbox":   {"agent_switch_failure_outbox", "", nil, true},
	"builtin:agent_switch_failure_receipts": {"agent_switch_failure_receipts", "", nil, true},
}

func maintenanceMillis(t time.Time) float64 {
	return float64(time.Since(t)) / float64(time.Millisecond)
}
func maintenanceHash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func maintenanceRetain(result *HistoryMaintenanceResult, err error) bool {
	var why maintenanceRetained
	if errors.As(err, &why) {
		result.Retained[string(why)]++
		return true
	}
	return false
}
func maintenanceKey(ctx context.Context, db maintenanceDB, stage maintenanceStage) (string, bool, error) {
	if !stage.builtin {
		return "id", stage.table == "conversation_provider_events", nil
	}
	rows, err := db.QueryContext(ctx, `PRAGMA table_info("`+stage.table+`")`)
	if err != nil {
		return "", false, err
	}
	defer rows.Close()
	key, typ := "", ""
	for rows.Next() {
		var n, nn, pk int
		var name, t string
		var def any
		if err = rows.Scan(&n, &name, &t, &nn, &def, &pk); err != nil {
			return "", false, err
		}
		if pk > 0 {
			if pk != 1 || key != "" {
				return "", false, errors.New("unsupported maintenance primary key")
			}
			key, typ = name, t
		}
	}
	if err = rows.Err(); err != nil {
		return "", false, err
	}
	if key == "" || (typ != "INTEGER" && typ != "TEXT") {
		return "", false, errors.New("unsupported maintenance primary key")
	}
	for _, r := range key {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return "", false, errors.New("unsupported maintenance primary key")
		}
	}
	return key, typ == "INTEGER", nil
}

// MaintainHistoryBatch is the sole mutation path. Candidate selection uses the
// reader pool before attempting admission. Maintenance never queues ahead of
// business writers, and never keeps its writer reservation between requests.
func (s *Store) MaintainHistoryBatch(ctx context.Context, req HistoryMaintenanceRequest, o HistoryMaintenanceOptions) (HistoryMaintenanceResult, error) {
	return s.maintainHistoryBatchAt(ctx, req, o, time.Now().UTC())
}

// MaintainHistorySnapshotBatch is only used by the copy-only offline runner.
// A historical clock can restrict eligibility but cannot move it into the future.
// This does not establish that an arbitrary database is offline or safe to write.
func (s *Store) MaintainHistorySnapshotBatch(ctx context.Context, req HistoryMaintenanceRequest, o HistoryMaintenanceOptions, asOf time.Time) (HistoryMaintenanceResult, error) {
	if asOf.IsZero() || asOf.After(time.Now().UTC()) {
		return HistoryMaintenanceResult{Status: "failed", CommitState: "not_started", Stage: req.Stage, NextCursor: req.Cursor, Retained: map[string]int{}}, errors.New("invalid historical snapshot clock")
	}
	return s.maintainHistoryBatchAt(ctx, req, o, asOf.UTC())
}

func (s *Store) maintainHistoryBatchAt(ctx context.Context, req HistoryMaintenanceRequest, o HistoryMaintenanceOptions, now time.Time) (result HistoryMaintenanceResult, err error) {
	result = HistoryMaintenanceResult{Status: "deferred", CommitState: "not_started", Stage: req.Stage, NextCursor: req.Cursor, Retained: map[string]int{}}
	defer func() {
		if err != nil {
			result.Status = "failed"
			result.ErrorType = "maintenance_failed"
			var coded interface{ Code() int }
			if errors.As(err, &coded) {
				result.SQLiteCode = coded.Code()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				result.ErrorType = "deadline_exceeded"
			}
			if errors.Is(err, context.Canceled) {
				result.ErrorType = "context_canceled"
			}
			result.NextCursor = req.Cursor
			result.Complete = false
			result.Committed = false
			result.Compacted = 0
			result.Deleted = 0
			result.Audited = 0
			result.Markers = 0
			result.LogicalBytes = 0
		}
	}()
	if s.readOnly {
		return result, errors.New("read-only maintenance store")
	}
	if err = o.Validate(); err != nil {
		return result, err
	}
	if req.BatchRows < 0 || req.BatchBytes < 0 || req.MaxRowBytes < 0 || req.ReadBudgetNS < 0 || req.WriteBudgetNS < 0 {
		return result, errors.New("invalid request limits")
	}
	if req.BatchRows > 0 {
		o.BatchRows = min(o.BatchRows, req.BatchRows)
	}
	if req.BatchBytes > 0 {
		o.BatchBytes = min(o.BatchBytes, req.BatchBytes)
	}
	if req.MaxRowBytes > 0 {
		o.MaxRowBytes = min(o.MaxRowBytes, req.MaxRowBytes)
	}
	o.MaxRowBytes = min(o.MaxRowBytes, o.BatchBytes)
	if req.ReadBudgetNS > 0 {
		o.ReadBudget = min(o.ReadBudget, time.Duration(req.ReadBudgetNS))
	}
	if req.WriteBudgetNS > 0 {
		o.WriteBudget = min(o.WriteBudget, time.Duration(req.WriteBudgetNS))
	}
	stage, ok := maintenanceStages[req.Stage]
	if !ok {
		return result, errors.New("unsupported maintenance stage")
	}
	cutoff, err := maintenanceTime(req.Cutoff)
	if err != nil {
		return result, err
	}
	if cutoff.After(now.Add(-o.Retention)) {
		return result, errors.New("maintenance cutoff is too recent")
	}
	// Separate, non-queued admission bounds all reader/CPU preparation memory.
	// It never reserves the business writer while a reader or JSON decoder waits.
	if !s.maintenanceBusy.CompareAndSwap(false, true) {
		result.Retained["maintenance_busy"]++
		return result, nil
	}
	defer s.maintenanceBusy.Store(false)
	readStarted := time.Now()
	readCtx, cancelRead := context.WithTimeout(ctx, o.ReadBudget)
	var key string
	var plans []maintenancePrepared
	func() {
		defer cancelRead()
		defer func() { result.ReadMS = maintenanceMillis(readStarted) }()
		cap := s.MaintenanceCapability(readCtx)
		if cap["schemaReady"] != true {
			err = errors.New("retention schema unavailable")
			return
		}
		if fmt.Sprint(cap["dbDevice"]) != fmt.Sprint(req.DatabaseDevice) || fmt.Sprint(cap["dbInode"]) != fmt.Sprint(req.DatabaseInode) {
			err = errors.New("maintenance database identity changed")
			return
		}
		// Metadata size and body materialization share one bounded read-only
		// snapshot, so a concurrent growing record cannot bypass byte limits.
		readTx, e := s.readDB.BeginTx(readCtx, &sql.TxOptions{ReadOnly: true})
		if e != nil {
			err = e
			return
		}
		defer readTx.Rollback()
		var numeric bool
		key, numeric, err = maintenanceKey(readCtx, readTx, stage)
		if err != nil {
			return
		}
		cursor := any(req.Cursor)
		if numeric {
			if req.Cursor == "" {
				cursor = int64(0)
			} else {
				var v int64
				if _, err = fmt.Sscan(req.Cursor, &v); err != nil || fmt.Sprint(v) != req.Cursor {
					err = errors.New("invalid numeric cursor")
					return
				}
				cursor = v
			}
		}
		rows, e := readTx.QueryContext(readCtx, `SELECT `+maintenanceQuoted(key)+` FROM `+maintenanceQuoted(stage.table)+` WHERE `+maintenanceQuoted(key)+`>? ORDER BY `+maintenanceQuoted(key)+` LIMIT ?`, cursor, o.BatchRows)
		if e != nil {
			err = e
			return
		}
		ids := []any{}
		for rows.Next() {
			var id any
			if err = rows.Scan(&id); err != nil {
				break
			}
			ids = append(ids, id)
		}
		if err == nil {
			err = rows.Err()
		}
		rows.Close()
		if err != nil {
			return
		}
		if len(ids) == 0 {
			result.Complete = true
			result.NextCursor = ""
			result.Status = "ok"
			return
		}
		plans, err = prepareMaintenanceRows(readCtx, readTx, stage, req.Stage, key, ids, o, now, cutoff)
		if err == nil {
			err = readTx.Commit()
		}
	}()
	if err != nil || result.Complete {
		return result, err
	}
	// Every preparation SELECT is closed above. No read snapshot is pinned while
	// waiting for the business writer, and no parsing/hash work occurs below.
	waitStarted := time.Now()
	if !s.writeMu.TryMaintenance() {
		result.WaitMS = maintenanceMillis(waitStarted)
		result.Retained["business_writer_busy"]++
		return result, nil
	}
	defer s.writeMu.Unlock()
	reservedAt := time.Now()
	defer func() { result.WriterReservedMS = maintenanceMillis(reservedAt) }()
	writeCtx, cancel := context.WithTimeout(ctx, o.WriteBudget)
	defer cancel()
	connection, err := s.writeDB.Conn(writeCtx)
	if err != nil {
		return result, err
	}
	defer connection.Close()
	err = withMaintenanceConnection(writeCtx, connection, &result, func() error {
		tx, e := connection.BeginTx(writeCtx, nil)
		result.WaitMS = maintenanceMillis(waitStarted)
		if e != nil {
			return e
		}
		heldAt := time.Now()
		defer func() { result.HeldMS = maintenanceMillis(heldAt) }()
		result.CommitState = "open"
		defer func() {
			rollbackErr := tx.Rollback()
			if result.CommitState == "open" {
				if rollbackErr == nil {
					result.CommitState = "rolled_back"
				} else {
					result.CommitState = "unknown"
				}
			}
		}()
		if e = validateHistoryRetentionSchema(writeCtx, tx); e != nil {
			return e
		}
		if e = validateMaintenancePolicy(writeCtx, tx); e != nil {
			return e
		}
		for _, plan := range plans {
			if writeCtx.Err() != nil {
				break
			}
			if s.writeMu.BusinessWaiting() {
				break
			}
			advanced, e := applyMaintenancePrepared(writeCtx, tx, stage, req.Stage, key, plan, cutoff, now, &result)
			if e != nil {
				return e
			}
			result.Scanned++
			if !advanced {
				break
			}
			result.NextCursor = maintenanceID(plan.id)
		}
		if e = writeCtx.Err(); e != nil {
			return e
		}
		commitAt := time.Now()
		result.CommitState = "unknown"
		if e = tx.Commit(); e != nil {
			return e
		}
		result.CommitMS = maintenanceMillis(commitAt)
		result.Committed = true
		result.CommitState = "committed"
		result.Status = "ok"
		return nil
	})
	return result, err
}

func maintenanceTransform(ctx context.Context, row maintenanceRow, stage, at, cutoff string) (map[string]any, []string, error) {
	out := map[string]any{}
	switch stage {
	case "raw":
		v, err := maintenanceJSONContext(ctx, row.text("payload_json"))
		if err != nil {
			return nil, nil, err
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, nil, maintenanceRetained("raw_not_object")
		}
		if prev, present := obj["_retention"]; present {
			m, ok := prev.(map[string]any)
			if !ok || m["protocol"] != "ao-history-v1" {
				return nil, nil, maintenanceRetained("foreign_retention_marker")
			}
			return nil, nil, maintenanceRetained("unproven_inline_audit")
		}
		fields := []string{}
		for _, key := range maintenanceBodyKeys {
			value, yes := obj[key]
			if !yes {
				continue
			}
			changed := false
			switch x := value.(type) {
			case string:
				changed = x != ""
				obj[key] = ""
			case map[string]any:
				changed = len(x) > 0
				obj[key] = map[string]any{}
			case []any:
				changed = len(x) > 0
				obj[key] = []any{}
			}
			if changed {
				fields = append(fields, key)
			}
		}
		for _, key := range maintenanceBodyKeys {
			if maintenanceTruthy(obj[key]) {
				return nil, nil, maintenanceRetained("unsupported_raw_body_type")
			}
		}
		if len(fields) == 0 {
			return out, fields, nil
		}
		fieldMapSHA, err := rawFieldMapSHA(row.text("payload_json"))
		if err != nil {
			return nil, nil, err
		}
		obj["_retention"] = map[string]any{"field_map_audit": rawAuditBinding(row, fieldMapSHA), "protocol": "ao-history-v1", "expired_at": at, "cutoff_at": cutoff, "fields": fields, "original_bytes": len(row.text("payload_json")), "original_sha256": maintenanceHash(row.text("payload_json"))}
		text, err := maintenanceCanonical(obj)
		if err != nil {
			return nil, nil, err
		}
		out["payload_json"] = text
		return out, fields, nil
	case "message":
		if row.text("text") != "" {
			out["text"] = maintenanceExpired
		}
		if row.text("delivery_content_json") != "" {
			out["delivery_content_json"] = ""
		}
	case "activity":
		if row.text("detail_json") != "" {
			if _, err := maintenanceJSONContext(ctx, row.text("detail_json")); err != nil {
				return nil, nil, err
			}
		}
		out = map[string]any{"summary": maintenanceExpired, "detail_json": `{"_ao_history_expired":true}`, "command_output": "", "streamed_text": ""}
	case "turn":
		for _, k := range []string{"diff_json", "plan_json"} {
			if row.text(k) != "" {
				out[k] = ""
			}
		}
		if x := row.text("error_message"); x != "" && x != maintenanceExpired && x != "controller ended before the turn completed" {
			out["error_message"] = maintenanceExpired
		}
	case "session":
		if row.text("prompt") != "" {
			out["prompt"] = maintenanceExpired
		}
	case "outputs":
		for _, k := range []string{"command_output", "streamed_text"} {
			if row.text(k) != "" {
				out[k] = ""
				if _, yes := row[k+"_truncated"]; yes {
					out[k+"_truncated"] = int64(1)
				}
			}
		}
	}
	fields := []string{}
	for k := range out {
		if !strings.HasSuffix(k, "_truncated") {
			fields = append(fields, k)
		}
	}
	sort.Strings(fields)
	return out, fields, nil
}
func prepareMaintenanceAudits(ctx context.Context, row maintenanceRow, stage string, fields []string, at, cutoff string) ([]maintenanceAudit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body := map[string]string{}
	if stage == "raw" {
		// Raw changes carry both audit digest domains inline. No mutation needs no durable audit.
		return nil, nil
	} else {
		for _, name := range fields {
			body[name] = row.text(name)
		}
	}
	bytes := int64(0)
	for _, value := range body {
		bytes += int64(len(value))
	}
	original, err := maintenanceCanonical(body)
	if err != nil {
		return nil, err
	}
	fieldJSON, err := maintenanceCanonical(fields)
	if err != nil {
		return nil, err
	}
	semantic, prompt := "", ""
	if stage == "message" {
		semantic = maintenanceHash(row.text("role") + "\x00" + row.text("text"))
		if row.text("role") == "user" {
			prompt = maintenanceHash(row.text("text"))
		}
	}
	if stage == "activity" {
		semantic = maintenanceHash(strings.Join([]string{row.text("kind"), row.text("status"), row.text("summary"), row.text("detail_json")}, "\x00"))
	}
	audit := maintenanceAudit{kind: maintenanceStages[stage].kind, id: maintenanceID(row["id"]), conversation: row.text("conversation_id"), semantic: semantic, prompt: prompt, originalSHA: maintenanceHash(original), at: at, cutoff: cutoff, fields: fieldJSON, bytes: bytes}
	audits := []maintenanceAudit{audit}
	if stage == "message" && row.text("role") == "user" && maintenanceCoordination(row.text("text")) {
		audit.kind = "message_coordination"
		audit.fields = `["coordination"]`
		audits = append(audits, audit)
	}
	return audits, ctx.Err()
}
func insertMaintenanceAudits(ctx context.Context, tx *sql.Tx, audits []maintenanceAudit) (int64, error) {
	n := int64(0)
	for _, a := range audits {
		result, err := tx.ExecContext(ctx, `INSERT INTO ao_history_retention(object_kind,object_id,conversation_id,semantic_sha256,prompt_sha256,original_bytes,original_sha256,expired_at,cutoff_at,fields_json) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(object_kind,object_id) DO NOTHING`, a.kind, a.id, a.conversation, a.semantic, a.prompt, a.bytes, a.originalSHA, a.at, a.cutoff, a.fields)
		if err != nil {
			return 0, err
		}
		added, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		n += added
	}
	return n, nil
}

func maintenanceCoordination(text string) bool {
	value := strings.TrimSpace(text)
	return maintenanceDelivery(value) || strings.HasPrefix(value, "<ao-handoff-request") || strings.HasPrefix(value, "AO transferred the previous agent's context in hidden system instructions.")
}
func maintenanceBuiltin(ctx context.Context, tx *sql.Tx, stage maintenanceStage, key string, id any, cutoff, now time.Time) (bool, error) {
	// Select only the lifecycle metadata required by the installed janitors.
	columns := "created_at"
	inclusive := false
	when := cutoff
	switch stage.table {
	case "telemetry_event":
		columns = "occurred_at"
	case "agent_switch_failure_outbox":
		columns = "expires_at"
		when = now
		inclusive = true
	case "agent_switch_failure_receipts":
		columns = "retain_until"
		when = now
		inclusive = true
	case "notifications":
		columns = "dismissed_at,resolved_at,type,session_id"
	}
	if stage.table == "notifications" {
		columns = "CAST(dismissed_at AS TEXT),CAST(resolved_at AS TEXT),type,session_id"
	} else {
		columns = "CAST(" + columns + " AS TEXT)"
	}
	rows, err := tx.QueryContext(ctx, `SELECT `+columns+` FROM "`+stage.table+`" WHERE "`+key+`"=?`, id)
	if err != nil {
		return false, err
	}
	if !rows.Next() {
		err = rows.Err()
		rows.Close()
		return false, err
	}
	var value sql.NullString
	if stage.table == "notifications" {
		var resolved, typ, owner sql.NullString
		err = rows.Scan(&value, &resolved, &typ, &owner)
		rows.Close()
		if err != nil {
			return false, err
		}
		if !value.Valid {
			return false, nil
		}
		if resolved.Valid {
			value = resolved
		} else if typ.String == "needs_input" {
			ok, e := maintenanceExists(ctx, tx, `SELECT 1 FROM sessions WHERE id=? AND (is_terminated=1 OR activity_state NOT IN ('waiting_input','blocked'))`, owner.String)
			if e != nil || !ok {
				return false, e
			}
		} else {
			return false, nil
		}
	} else {
		err = rows.Scan(&value)
		rows.Close()
		if err != nil {
			return false, err
		}
	}
	if !value.Valid {
		return false, nil
	}
	old, err := maintenanceOld(value.String, when, inclusive)
	if err != nil || !old {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM "`+stage.table+`" WHERE "`+key+`"=?`, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

func maintenanceDelivery(text string) bool { _, ok := domain.CoordinationDeliveryID(text); return ok }

func (s *Store) MaintenanceCapability(ctx context.Context) map[string]any {
	cap := s.historyRetentionCapability(ctx)
	if err := validateMaintenancePolicy(ctx, s.readDB); err != nil {
		cap["schemaReady"] = false
	}
	return cap
}
func validateMaintenancePolicy(ctx context.Context, db maintenanceDB) error {
	var encoding string
	if err := db.QueryRowContext(ctx, "PRAGMA encoding").Scan(&encoding); err != nil {
		return err
	}
	if encoding != "UTF-8" {
		return errors.New("unsupported maintenance encoding")
	}
	var version int
	if err := db.QueryRowContext(ctx, `SELECT max(version_id) FROM goose_db_version WHERE is_applied=1`).Scan(&version); err != nil {
		return err
	}
	if version != 191 && version != 192 {
		return errors.New("unsupported maintenance schema")
	}
	return nil
}

// Register recovery before changing any connection-local setting. Even a driver
// that applies PRAGMA then reports cancellation cannot leak fail-fast policy
// into the next business writer. Cleanup shares the original deadline; a spent
// budget discards the connection rather than adding a second wait window.
func withMaintenanceConnection(ctx context.Context, connection *sql.Conn, result *HistoryMaintenanceResult, run func() error) error {
	var priorBusy int
	if err := connection.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&priorBusy); err != nil {
		return err
	}
	defer func() {
		if _, restoreErr := connection.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", priorBusy)); restoreErr != nil {
			result.ConnectionRestoreFailed = true
			discarded := connection.Raw(func(any) error { return driver.ErrBadConn })
			result.ConnectionDiscarded = errors.Is(discarded, driver.ErrBadConn)
			result.Retained["writer_connection_restore_failed"]++
		}
	}()
	if _, err := connection.ExecContext(ctx, "PRAGMA busy_timeout=0"); err != nil {
		return err
	}
	return run()
}

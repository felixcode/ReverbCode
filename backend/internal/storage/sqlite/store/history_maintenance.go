package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
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
	if o.Retention <= 0 || o.BatchRows <= 0 || o.BatchBytes <= 0 || o.MaxRowBytes <= 0 || o.ReadBudget <= 0 || o.WriteBudget <= 0 {
		return errors.New("invalid maintenance settings")
	}
	return nil
}

type HistoryMaintenanceResult struct {
	Status       string         `json:"status"`
	Stage        string         `json:"stage"`
	NextCursor   string         `json:"next_cursor"`
	Complete     bool           `json:"complete"`
	Scanned      int            `json:"scanned"`
	Compacted    int            `json:"compacted"`
	Deleted      int            `json:"deleted"`
	Audited      int            `json:"audited"`
	Markers      int64          `json:"markers"`
	LogicalBytes int64          `json:"logical_bytes_delta"`
	Retained     map[string]int `json:"retained"`
	Committed    bool           `json:"transaction_committed"`
	ReadMS       float64        `json:"read_ms"`
	WaitMS       float64        `json:"wait_ms"`
	HeldMS       float64        `json:"held_ms"`
	CommitMS     float64        `json:"commit_ms"`
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
func (s *Store) MaintainHistoryBatch(ctx context.Context, req HistoryMaintenanceRequest, o HistoryMaintenanceOptions) (result HistoryMaintenanceResult, err error) {
	result = HistoryMaintenanceResult{Status: "deferred", Stage: req.Stage, NextCursor: req.Cursor, Retained: map[string]int{}}
	defer func() {
		if err != nil {
			result.Status = "failed"
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
	stage, ok := maintenanceStages[req.Stage]
	if !ok {
		return result, errors.New("unsupported maintenance stage")
	}
	cutoff, err := maintenanceTime(req.Cutoff)
	if err != nil {
		return result, err
	}
	now := time.Now().UTC()
	if cutoff.After(now.Add(-o.Retention)) {
		return result, errors.New("maintenance cutoff is too recent")
	}
	cap := s.historyRetentionCapability(ctx)
	if cap["schemaReady"] != true {
		return result, errors.New("retention schema unavailable")
	}
	// Device/inode bind the private socket request to the database originally
	// inspected by the scheduler. No arbitrary database path is accepted.
	if fmt.Sprint(cap["dbDevice"]) != fmt.Sprint(req.DatabaseDevice) || fmt.Sprint(cap["dbInode"]) != fmt.Sprint(req.DatabaseInode) {
		return result, errors.New("maintenance database identity changed")
	}
	readStarted := time.Now()
	readCtx, cancelRead := context.WithTimeout(ctx, o.ReadBudget)
	key, numeric, err := maintenanceKey(readCtx, s.readDB, stage)
	if err != nil {
		cancelRead()
		return result, err
	}
	cursor := any(req.Cursor)
	if numeric {
		if req.Cursor == "" {
			cursor = int64(0)
		} else {
			var v int64
			if _, err = fmt.Sscan(req.Cursor, &v); err != nil || fmt.Sprint(v) != req.Cursor {
				cancelRead()
				return result, errors.New("invalid numeric cursor")
			}
			cursor = v
		}
	}
	rows, err := s.readDB.QueryContext(readCtx, `SELECT "`+key+`" FROM "`+stage.table+`" WHERE "`+key+`">? ORDER BY "`+key+`" LIMIT ?`, cursor, o.BatchRows)
	if err != nil {
		cancelRead()
		return result, err
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
	cancelRead()
	result.ReadMS = maintenanceMillis(readStarted)
	if err != nil {
		return result, err
	}
	if len(ids) == 0 {
		result.Status = "ok"
		result.Complete = true
		result.NextCursor = ""
		return result, nil
	}
	waitStarted := time.Now()
	if !s.writeMu.TryMaintenance() {
		result.WaitMS = maintenanceMillis(waitStarted)
		result.Retained["business_writer_busy"]++
		return result, nil
	}
	defer s.writeMu.Unlock()
	writeCtx, cancel := context.WithTimeout(ctx, o.WriteBudget)
	defer cancel()
	connection, err := s.writeDB.Conn(writeCtx)
	if err != nil {
		return result, err
	}
	defer connection.Close()
	var priorBusy int
	if err = connection.QueryRowContext(writeCtx, "PRAGMA busy_timeout").Scan(&priorBusy); err != nil {
		return result, err
	}
	if _, err = connection.ExecContext(writeCtx, "PRAGMA busy_timeout=0"); err != nil {
		return result, err
	}
	defer func() {
		restoreCtx, stop := context.WithTimeout(context.Background(), o.ReadBudget)
		defer stop()
		if _, restoreErr := connection.ExecContext(restoreCtx, fmt.Sprintf("PRAGMA busy_timeout=%d", priorBusy)); restoreErr != nil {
			_ = connection.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	tx, err := connection.BeginTx(writeCtx, nil)
	result.WaitMS = maintenanceMillis(waitStarted)
	if err != nil {
		return result, err
	}
	heldStarted := time.Now()
	defer func() { result.HeldMS = maintenanceMillis(heldStarted) }()
	defer tx.Rollback()
	used := int64(0)
	for _, id := range ids {
		if writeCtx.Err() != nil {
			break
		}
		// Never start another object once a business operation is waiting.
		if s.writeMu.BusinessWaiting() {
			break
		}
		if !stage.builtin {
			marked, e := maintenanceExists(writeCtx, tx, `SELECT 1 FROM ao_history_retention WHERE object_kind=? AND object_id=?`, stage.kind, maintenanceID(id))
			if e != nil {
				return result, e
			}
			if marked {
				result.Scanned++
				result.NextCursor = maintenanceID(id)
				result.Retained["already_expired"]++
				continue
			}
			sizeExpr := []string{}
			for _, f := range stage.fields {
				sizeExpr = append(sizeExpr, `COALESCE(length(CAST("`+f+`" AS BLOB)),0)`)
			}
			var size int64
			e = tx.QueryRowContext(writeCtx, `SELECT `+strings.Join(sizeExpr, "+")+` FROM "`+stage.table+`" WHERE id=?`, id).Scan(&size)
			if errors.Is(e, sql.ErrNoRows) {
				result.Scanned++
				result.NextCursor = maintenanceID(id)
				continue
			}
			if e != nil {
				return result, e
			}
			if size > o.MaxRowBytes {
				result.Retained["row_size_limit"]++
				result.Scanned++
				result.NextCursor = maintenanceID(id)
				continue
			}
			if used+size > o.BatchBytes {
				break
			}
			used += size
		}
		if stage.builtin {
			changed, e := maintenanceBuiltin(writeCtx, tx, stage, key, id, cutoff, now)
			if e != nil {
				if !maintenanceRetain(&result, e) {
					return result, e
				}
			} else if changed {
				result.Deleted++
			}
		} else {
			row, e := maintenanceRead(writeCtx, tx, stage.table, id)
			if e != nil {
				if !maintenanceRetain(&result, e) {
					return result, e
				}
			} else if row != nil {
				eligible, e := maintenanceEligible(writeCtx, tx, row, req.Stage, cutoff)
				if e != nil {
					if !maintenanceRetain(&result, e) {
						return result, e
					}
				} else if !eligible {
					result.Retained["protected_owner_or_turn"]++
				} else {
					updates, fields, e := maintenanceTransform(row, req.Stage, now.Format(time.RFC3339Nano), cutoff.Format(time.RFC3339Nano))
					if e != nil {
						if !maintenanceRetain(&result, e) {
							return result, e
						}
					} else {
						if len(fields) > 0 {
							keys := []string{}
							for k := range updates {
								keys = append(keys, k)
							}
							sort.Strings(keys)
							assign := []string{}
							args := []any{}
							for _, k := range keys {
								assign = append(assign, `"`+k+`"=?`)
								args = append(args, updates[k])
							}
							if req.Stage == "message" || req.Stage == "activity" || req.Stage == "outputs" {
								assign = append(assign, "revision=revision+1")
							}
							args = append(args, id)
							if _, e = tx.ExecContext(writeCtx, `UPDATE "`+stage.table+`" SET `+strings.Join(assign, ",")+` WHERE id=?`, args...); e != nil {
								return result, e
							}
							for k, v := range updates {
								result.LogicalBytes += int64(len(row.text(k)) - len(fmt.Sprint(v)))
							}
							result.Compacted++
						} else {
							result.Audited++
						}
						n, e := maintenanceMarker(writeCtx, tx, row, req.Stage, fields, now.Format(time.RFC3339Nano), cutoff.Format(time.RFC3339Nano))
						if e != nil {
							return result, e
						}
						result.Markers += n
					}
				}
			}
		}
		result.Scanned++
		result.NextCursor = maintenanceID(id)
	}
	// A canceled SQL/transaction is failed, not a successful partial batch. The
	// caller must not advance its durable cursor from an uncommitted response.
	if err = writeCtx.Err(); err != nil {
		return result, err
	}
	commitStarted := time.Now()
	if err = tx.Commit(); err != nil {
		return result, err
	}
	result.CommitMS = maintenanceMillis(commitStarted)
	result.Committed = true
	result.Status = "ok"
	return result, nil
}
func maintenanceTransform(row maintenanceRow, stage, at, cutoff string) (map[string]any, []string, error) {
	out := map[string]any{}
	switch stage {
	case "raw":
		v, err := maintenanceJSON(row.text("payload_json"))
		if err != nil {
			return nil, nil, err
		}
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, nil, maintenanceRetained("raw_not_object")
		}
		if prev := obj["_retention"]; prev != nil {
			m, ok := prev.(map[string]any)
			if !ok || !(m["protocol"] == "ao-history-v1" || m["protocol"] == json.Number("1") && m["contentExpired"] == true) {
				return nil, nil, maintenanceRetained("foreign_retention_marker")
			}
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
		if len(fields) == 0 {
			return out, fields, nil
		}
		obj["_retention"] = map[string]any{"protocol": "ao-history-v1", "expired_at": at, "cutoff_at": cutoff, "fields": fields, "original_bytes": len(row.text("payload_json")), "original_sha256": maintenanceHash(row.text("payload_json"))}
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
			if _, err := maintenanceJSON(row.text("detail_json")); err != nil {
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
func maintenanceMarker(ctx context.Context, tx *sql.Tx, row maintenanceRow, stage string, fields []string, at, cutoff string) (int64, error) {
	body := map[string]string{}
	if stage == "raw" {
		body["payload_json"] = row.text("payload_json")
	} else {
		for _, k := range fields {
			body[k] = row.text(k)
		}
	}
	bytes := 0
	for _, v := range body {
		bytes += len(v)
	}
	original, err := maintenanceCanonical(body)
	if err != nil {
		return 0, err
	}
	fieldJSON, err := maintenanceCanonical(fields)
	if err != nil {
		return 0, err
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
	kind := maintenanceStages[stage].kind
	conv := row.text("conversation_id")
	res, err := tx.ExecContext(ctx, `INSERT INTO ao_history_retention VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(object_kind,object_id) DO NOTHING`, kind, maintenanceID(row["id"]), conv, semantic, prompt, bytes, maintenanceHash(original), at, cutoff, fieldJSON)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if stage == "message" && row.text("role") == "user" && maintenanceCoordination(row.text("text")) {
		res, err = tx.ExecContext(ctx, `INSERT INTO ao_history_retention VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(object_kind,object_id) DO NOTHING`, "message_coordination", maintenanceID(row["id"]), conv, semantic, prompt, bytes, maintenanceHash(original), at, cutoff, `["coordination"]`)
		if err != nil {
			return 0, err
		}
		m, e := res.RowsAffected()
		return n + m, e
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
	return s.historyRetentionCapability(ctx)
}

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const maintenanceExpired = "[AO history expired]"

type maintenanceRetained string

func (e maintenanceRetained) Error() string { return string(e) }

type maintenanceRow map[string]any

func (r maintenanceRow) text(k string) string   { v, _ := r[k].(string); return v }
func (r maintenanceRow) integer(k string) int64 { v, _ := r[k].(int64); return v }

type maintenanceDB interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func maintenanceRead(ctx context.Context, db maintenanceDB, table string, id any) (maintenanceRow, error) {
	// table/key are selected exclusively by the closed maintenance stage registry.
	columns := ""
	switch table {
	case "sessions":
		columns = "id,is_terminated,prompt,CAST(updated_at AS TEXT) AS updated_at"
	case "conversations":
		columns = "id,scope,session_id,current_session_id"
	case "conversation_turns":
		columns = "id,conversation_id,handled_by_session_id,state,diff_json,plan_json,error_message,CAST(completed_at AS TEXT) AS completed_at"
	case "conversation_provider_events":
		columns = "id,conversation_id,session_id,payload_json,CAST(received_at AS TEXT) AS received_at"
	case "conversation_messages":
		columns = "id,conversation_id,turn_id,role,text,streaming,delivery_content_json,CAST(created_at AS TEXT) AS created_at"
	case "conversation_activities":
		columns = "id,conversation_id,turn_id,kind,status,summary,detail_json,command_output,streamed_text,command_output_truncated,streamed_text_truncated,CAST(created_at AS TEXT) AS created_at"
	default:
		return nil, errors.New("unsupported maintenance row")
	}
	rows, err := db.QueryContext(ctx, `SELECT `+columns+` FROM "`+table+`" WHERE id=?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, rows.Err()
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err = rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	row := maintenanceRow{}
	for i, v := range vals {
		if b, ok := v.([]byte); ok {
			v = string(b)
		}
		if s, ok := v.(string); ok && !utf8.ValidString(s) {
			return nil, maintenanceRetained("invalid_utf8_text")
		}
		row[cols[i]] = v
	}
	return row, nil
}
func maintenanceExists(ctx context.Context, db maintenanceDB, q string, args ...any) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, q, args...).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Match the existing strict Python nanosecond age proof. Unknown timestamps are
// retained, never normalized into an apparently old lifecycle value.
var maintenanceStamp = regexp.MustCompile(`^([0-9]{4}-[0-9]{2}-[0-9]{2})([T ])([0-9]{2}:[0-9]{2}:[0-9]{2})(\.[0-9]{1,9})?(Z| ?[+-][0-9]{2}:?[0-9]{2}( [A-Za-z][A-Za-z0-9_+\-]*)?)?$`)

func maintenanceTime(value string) (time.Time, error) {
	m := maintenanceStamp.FindStringSubmatch(value)
	if m == nil || strings.HasPrefix(value, "0001-") || strings.HasPrefix(value, "0000-") {
		return time.Time{}, maintenanceRetained("invalid_timestamp")
	}
	zone := strings.TrimSpace(m[5])
	if zone == "" {
		if m[2] != " " {
			return time.Time{}, maintenanceRetained("invalid_timestamp")
		}
		zone = "Z"
	}
	parts := strings.Split(zone, " ")
	zone = parts[0]
	if len(parts) > 1 && (parts[1] == "UTC" || parts[1] == "GMT") && zone != "+00:00" && zone != "-00:00" && zone != "+0000" && zone != "-0000" {
		return time.Time{}, maintenanceRetained("invalid_timestamp")
	}
	if len(zone) > 1 {
		h, e := strconv.Atoi(zone[1:3])
		minute, minuteErr := strconv.Atoi(zone[len(zone)-2:])
		if e != nil || h > 23 || minuteErr != nil || minute > 59 {
			return time.Time{}, maintenanceRetained("invalid_timestamp")
		}
	}
	if len(zone) == 5 && zone != "Z" {
		zone = zone[:3] + ":" + zone[3:]
	}
	t, err := time.Parse(time.RFC3339Nano, m[1]+"T"+m[3]+m[4]+zone)
	if err != nil {
		return time.Time{}, maintenanceRetained("invalid_timestamp")
	}
	return t, nil
}
func maintenanceOld(value string, cutoff time.Time, inclusive bool) (bool, error) {
	t, err := maintenanceTime(value)
	return err == nil && (t.Before(cutoff) || (inclusive && t.Equal(cutoff))), err
}
func maintenanceTerminal(state string) bool {
	switch state {
	case "completed", "failed", "interrupted", "recovered", "cancelled":
		return true
	}
	return false
}
func maintenancePendingSwitch(ctx context.Context, db maintenanceDB, owner string) (bool, error) {
	return maintenanceExists(ctx, db, `SELECT 1 FROM agent_switches WHERE session_id=? AND state NOT IN ('completed','failed') LIMIT 1`, owner)
}
func maintenanceClosedOwner(ctx context.Context, db maintenanceDB, id string) (bool, error) {
	c, err := maintenanceRead(ctx, db, "conversations", id)
	if err != nil || c == nil {
		return false, err
	}
	if c.text("scope") != "session" || c.text("session_id") == "" {
		return false, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT session_id FROM conversation_branches WHERE conversation_id=? UNION SELECT ? UNION SELECT ?`, id, c.text("session_id"), c.text("current_session_id"))
	if err != nil {
		return false, err
	}
	owners := []string{}
	for rows.Next() {
		var owner sql.NullString
		if err = rows.Scan(&owner); err != nil {
			rows.Close()
			return false, err
		}
		if owner.Valid && owner.String != "" {
			owners = append(owners, owner.String)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, owner := range owners {
		handoff, e := maintenancePendingSwitch(ctx, db, owner)
		if e != nil || handoff {
			return false, e
		}
		reviewing, e := maintenanceExists(ctx, db, `SELECT 1 FROM review_run WHERE session_id=? AND status='running' LIMIT 1`, owner)
		if e != nil || reviewing {
			return false, e
		}
		var terminated int
		if err = db.QueryRowContext(ctx, `SELECT is_terminated FROM sessions WHERE id=?`, owner).Scan(&terminated); errors.Is(err, sql.ErrNoRows) {
			return false, nil
		} else if err != nil {
			return false, err
		}
		if terminated != 1 {
			return false, nil
		}
	}
	active, err := maintenanceExists(ctx, db, `SELECT 1 FROM conversation_turns WHERE conversation_id=? AND state IN ('queued','running') LIMIT 1`, id)
	if err != nil || active {
		return false, err
	}
	pending, err := maintenanceExists(ctx, db, `SELECT 1 FROM conversation_activities WHERE conversation_id=? AND kind IN ('approval','user_input') AND status='pending' LIMIT 1`, id)
	return !pending, err
}
func maintenanceSessionClosed(ctx context.Context, db maintenanceDB, owner string) (bool, error) {
	pending, err := maintenancePendingSwitch(ctx, db, owner)
	if err != nil || pending {
		return false, err
	}
	for _, q := range []string{`SELECT 1 FROM conversation_turns WHERE handled_by_session_id=? AND state IN ('queued','running') LIMIT 1`, `SELECT 1 FROM conversation_activities a JOIN conversations c ON c.id=a.conversation_id WHERE (c.session_id=? OR c.current_session_id=?) AND a.kind IN ('approval','user_input') AND a.status='pending' LIMIT 1`, `SELECT 1 FROM review_run WHERE session_id=? AND status='running' LIMIT 1`} {
		args := []any{owner}
		if strings.Contains(q, "current_session_id=?") {
			args = append(args, owner)
		}
		yes, e := maintenanceExists(ctx, db, q, args...)
		if e != nil || yes {
			return false, e
		}
	}
	rows, err := db.QueryContext(ctx, `SELECT id FROM conversations WHERE session_id=? OR current_session_id=? UNION SELECT conversation_id FROM conversation_branches WHERE session_id=?`, owner, owner, owner)
	if err != nil {
		return false, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return false, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		ok, e := maintenanceClosedOwner(ctx, db, id)
		if e != nil || !ok {
			return false, e
		}
	}
	return true, nil
}
func maintenanceEligible(ctx context.Context, db maintenanceDB, row maintenanceRow, stage string, cutoff time.Time) (bool, error) {
	if stage == "session" {
		old, err := maintenanceOld(row.text("updated_at"), cutoff, true)
		if err != nil || !old || row.integer("is_terminated") != 1 {
			return false, err
		}
		return maintenanceSessionClosed(ctx, db, row.text("id"))
	}
	var turn maintenanceRow
	if stage == "raw" {
		old, err := maintenanceOld(row.text("received_at"), cutoff, true)
		if err != nil || !old {
			return false, err
		}
		var ownerTerminated int
		err = db.QueryRowContext(ctx, `SELECT is_terminated FROM sessions WHERE id=?`, row.text("session_id")).Scan(&ownerTerminated)
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
			ownerTerminated = 0
		}
		if err != nil {
			return false, err
		}
		if ownerTerminated == 1 {
			closed, err := maintenanceClosedOwner(ctx, db, row.text("conversation_id"))
			if err != nil {
				return false, err
			}
			if closed {
				return true, nil
			}
			live, err := maintenanceExists(ctx, db, `SELECT 1 FROM conversation_turns WHERE handled_by_session_id=? AND state IN ('queued','running') LIMIT 1`, row.text("session_id"))
			if err != nil {
				return false, err
			}
			pending, err := maintenanceExists(ctx, db, `SELECT 1 FROM conversation_activities a JOIN conversation_turns t ON t.id=a.turn_id WHERE t.handled_by_session_id=? AND a.kind IN ('approval','user_input') AND a.status='pending' LIMIT 1`, row.text("session_id"))
			if err != nil {
				return false, err
			}
			handoff, err := maintenancePendingSwitch(ctx, db, row.text("session_id"))
			if err != nil {
				return false, err
			}
			reviewing, e := maintenanceExists(ctx, db, `SELECT 1 FROM review_run WHERE session_id=? AND status='running' LIMIT 1`, row.text("session_id"))
			if e != nil {
				return false, e
			}
			if reviewing {
				return false, maintenanceRetained("owner_review_running")
			}
			if !live && !pending && !handoff && !reviewing {
				return true, nil
			}
		}
		payload, err := maintenanceJSONContext(ctx, row.text("payload_json"))
		if err != nil {
			return false, err
		}
		obj, ok := payload.(map[string]any)
		if !ok {
			return false, maintenanceRetained("raw_not_object")
		}
		native, _ := obj["providerTurnId"].(string)
		if native == "" {
			for _, key := range maintenanceBodyKeys {
				if maintenanceTruthy(obj[key]) {
					return false, maintenanceRetained("raw_turn_unattributed")
				}
			}
			return true, nil
		}
		// Explicit partial-index predicate is essential for large native histories.
		rows, err := db.QueryContext(ctx, `SELECT id FROM conversation_turns WHERE conversation_id=? AND handled_by_session_id=? AND provider_turn_id=? AND provider_turn_id<>'' LIMIT 2`, row.text("conversation_id"), row.text("session_id"), native)
		if err != nil {
			return false, err
		}
		ids := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return false, err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return false, err
		}
		if len(ids) != 1 {
			return false, maintenanceRetained("raw_turn_unattributed")
		}
		turn, err = maintenanceTurn(ctx, db, ids[0])
		if err != nil {
			return false, err
		}
	} else if stage == "turn" {
		turn = row
	} else {
		if stage == "message" && row.integer("streaming") != 0 {
			return false, nil
		}
		if (stage == "outputs" || stage == "activity") && row.text("status") == "pending" {
			return false, nil
		}
		old, err := maintenanceOld(row.text("created_at"), cutoff, true)
		if err != nil || !old {
			return false, err
		}
		if row.text("turn_id") == "" {
			return maintenanceClosedOwner(ctx, db, row.text("conversation_id"))
		}
		turn, err = maintenanceTurn(ctx, db, row.text("turn_id"))
		if err != nil {
			return false, err
		}
		if turn != nil && turn.text("conversation_id") != row.text("conversation_id") {
			return false, maintenanceRetained("turn_unattributed")
		}
	}
	if turn == nil {
		return false, maintenanceRetained("turn_unattributed")
	}
	if !maintenanceTerminal(turn.text("state")) {
		return false, nil
	}
	old, err := maintenanceOld(turn.text("completed_at"), cutoff, true)
	if err != nil || !old {
		return false, err
	}
	handoff, err := maintenancePendingSwitch(ctx, db, turn.text("handled_by_session_id"))
	if err != nil {
		return false, err
	}
	if handoff {
		return false, maintenanceRetained("handoff_in_progress")
	}
	pending, err := maintenanceExists(ctx, db, `SELECT 1 FROM conversation_activities WHERE conversation_id=? AND turn_id=? AND kind IN ('approval','user_input') AND status='pending' LIMIT 1`, row.text("conversation_id"), turn.text("id"))
	if pending {
		return false, maintenanceRetained("pending_request")
	}
	return !pending, err
}

var maintenanceBodyKeys = []string{"delta", "detail", "diff", "input", "plan", "summary", "text"}

func maintenanceTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		n, _ := strconv.ParseFloat(string(x), 64)
		return n != 0
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	}
	return true
}

// Decoder preserves numbers and rejects duplicate keys at every nesting level.
func maintenanceJSON(value string) (any, error) {
	return maintenanceJSONContext(context.Background(), value)
}

func maintenanceJSONContext(ctx context.Context, value string) (any, error) {
	if !utf8.ValidString(value) || !maintenanceValidEscapes(value) {
		return nil, maintenanceRetained("invalid_json")
	}
	d := json.NewDecoder(strings.NewReader(value))
	d.UseNumber()
	var parse func() (any, error)
	parse = func() (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		tok, err := d.Token()
		if err != nil {
			return nil, maintenanceRetained("invalid_json")
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return tok, nil
		}
		switch delim {
		case '{':
			m := map[string]any{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return nil, maintenanceRetained("invalid_json")
				}
				key, ok := k.(string)
				if !ok {
					return nil, maintenanceRetained("invalid_json")
				}
				if _, yes := m[key]; yes {
					return nil, maintenanceRetained("duplicate_json_key")
				}
				v, e := parse()
				if e != nil {
					return nil, e
				}
				m[key] = v
			}
			_, err = d.Token()
			return m, err
		case '[':
			a := []any{}
			for d.More() {
				v, e := parse()
				if e != nil {
					return nil, e
				}
				a = append(a, v)
			}
			_, err = d.Token()
			return a, err
		}
		return nil, maintenanceRetained("invalid_json")
	}
	v, err := parse()
	if err != nil {
		return nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, maintenanceRetained("invalid_json")
	}
	return v, nil
}
func maintenanceCanonical(v any) (string, error) {
	var b strings.Builder
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return "", err
	}
	return maintenanceUnicodeJSON(strings.TrimSuffix(b.String(), "\n")), nil
}
func maintenanceID(v any) string {
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case string:
		return x
	}
	return fmt.Sprint(v)
}

func maintenanceValidEscapes(text string) bool {
	for i := 0; i < len(text); i++ {
		if text[i] != '\\' {
			continue
		}
		i++
		if i >= len(text) {
			return false
		}
		if text[i] != 'u' {
			continue
		}
		if i+4 >= len(text) {
			return false
		}
		n, e := strconv.ParseUint(text[i+1:i+5], 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if n >= 0xDC00 && n <= 0xDFFF {
			return false
		}
		if n >= 0xD800 && n <= 0xDBFF {
			if i+6 >= len(text) || text[i+1:i+3] != "\\u" {
				return false
			}
			m, e := strconv.ParseUint(text[i+3:i+7], 16, 16)
			if e != nil || m < 0xDC00 || m > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}

// encoding/json always escapes U+2028/2029; the existing Python audit contract
// uses ensure_ascii=False. Unescape only actual encoder escapes, never a
// user-authored literal backslash sequence.
func maintenanceUnicodeJSON(text string) string {
	var out strings.Builder
	for i := 0; i < len(text); {
		if text[i] != '\\' {
			out.WriteByte(text[i])
			i++
			continue
		}
		j := i
		for j < len(text) && text[j] == '\\' {
			j++
		}
		n := j - i
		if n%2 == 1 && j+5 <= len(text) && (text[j:j+5] == "u2028" || text[j:j+5] == "u2029") {
			out.WriteString(text[i : j-1])
			if text[j+4] == '8' {
				out.WriteRune('\u2028')
			} else {
				out.WriteRune('\u2029')
			}
			i = j + 5
		} else {
			out.WriteString(text[i:j])
			i = j
		}
	}
	return out.String()
}

// Eligibility must not expand another object's diff/plan/prompt while holding
// the writer. Only the lifecycle facts needed by this proof are selected.
func maintenanceTurn(ctx context.Context, db maintenanceDB, id string) (maintenanceRow, error) {
	var key, conv, owner, state string
	var completed sql.NullString
	err := db.QueryRowContext(ctx, `SELECT id,conversation_id,handled_by_session_id,state,CAST(completed_at AS TEXT) FROM conversation_turns WHERE id=?`, id).Scan(&key, &conv, &owner, &state, &completed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return maintenanceRow{"id": key, "conversation_id": conv, "handled_by_session_id": owner, "state": state, "completed_at": completed.String}, nil
}

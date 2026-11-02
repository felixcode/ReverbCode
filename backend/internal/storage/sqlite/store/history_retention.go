package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// EnsureHistoryRetentionSchema adds only the small compatibility sidecar. It does
// not rewrite existing application tables or advance the historical migration ledger.
func EnsureHistoryRetentionSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`CREATE TABLE IF NOT EXISTS ao_history_retention (
 object_kind TEXT NOT NULL, object_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
 semantic_sha256 TEXT NOT NULL, prompt_sha256 TEXT NOT NULL,
 original_bytes INTEGER NOT NULL, original_sha256 TEXT NOT NULL,
 expired_at TEXT NOT NULL, cutoff_at TEXT NOT NULL, fields_json TEXT NOT NULL,
 PRIMARY KEY(object_kind, object_id))`)
	if err != nil {
		return err
	}
	if err = validateHistoryRetentionSchema(context.Background(), tx); err != nil {
		return err
	}
	if _, err = tx.Exec(`CREATE INDEX IF NOT EXISTS ao_history_retention_conversation ON ao_history_retention(conversation_id)`); err != nil {
		return err
	}
	return tx.Commit()
}

type projectionDBKey struct{}

func (s *Store) retentionDB(ctx context.Context) gen.DBTX {
	if db, ok := ctx.Value(projectionDBKey{}).(gen.DBTX); ok && db != nil {
		return db
	}
	return s.readDB
}

func (s *Store) applyHistoryRetention(ctx context.Context, db gen.DBTX, conversationID string, snapshot *ConversationSnapshot) error {
	if s.historyRetentionAbsent {
		return nil
	}
	type marker struct{ semantic, prompt string }
	markers := map[string]marker{}
	groups := map[string][]string{"message": {}, "message_coordination": {}, "activity": {}, "turn": {}}
	for _, m := range snapshot.Messages {
		groups["message"] = append(groups["message"], m.ID)
		groups["message_coordination"] = append(groups["message_coordination"], m.ID)
	}
	for _, a := range snapshot.Activities {
		groups["activity"] = append(groups["activity"], a.ID)
	}
	for _, t := range snapshot.Turns {
		groups["turn"] = append(groups["turn"], t.ID)
	}
	for kind, ids := range groups {
		for start := 0; start < len(ids); start += 200 {
			end := start + 200
			if end > len(ids) {
				end = len(ids)
			}
			args := make([]any, 0, end-start+2)
			for _, id := range ids[start:end] {
				args = append(args, id)
			}
			args = append(args, kind, conversationID)
			// Keep the page IDs outermost so the existing (kind, id) primary key
			// wins over scanning every audit marker in this conversation.
			query := `WITH wanted(wanted_id) AS (VALUES ` + strings.TrimSuffix(strings.Repeat("(?),", end-start), ",") + `) SELECT object_kind,object_id,semantic_sha256,prompt_sha256 FROM wanted CROSS JOIN ao_history_retention ON object_id=wanted_id WHERE object_kind=? AND conversation_id=?`
			rows, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			for rows.Next() {
				var kind, id string
				var m marker
				if err = rows.Scan(&kind, &id, &m.semantic, &m.prompt); err != nil {
					rows.Close()
					return err
				}
				markers[kind+"\x00"+id] = m
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
	}

	for i := range snapshot.Messages {
		m := &snapshot.Messages[i]
		if r, ok := markers["message\x00"+m.ID]; ok {
			m.ContentExpired = true
			m.RetentionSemanticSHA256 = r.semantic
			m.RetentionPromptSHA256 = r.prompt
		}
		_, m.RetentionCoordination = markers["message_coordination\x00"+m.ID]
	}
	for i := range snapshot.Activities {
		a := &snapshot.Activities[i]
		if r, ok := markers["activity\x00"+a.ID]; ok {
			a.ContentExpired = true
			a.RetentionSemanticSHA256 = r.semantic
		}
	}
	for i := range snapshot.Turns {
		t := &snapshot.Turns[i]
		if _, ok := markers["turn\x00"+t.ID]; ok {
			t.ContentExpired = true
		}
	}
	return nil
}

// Only an already-expired logical turn can suppress incoming prose. This never
// matches by a global content hash, so identical messages in new turns survive.
// Column predicates make the existing partial native-ID indexes eligible.
const expiredProviderPayloadSQL = `SELECT EXISTS(
 SELECT 1 FROM conversation_turns t JOIN ao_history_retention r ON r.object_kind='turn' AND r.object_id=t.id WHERE ?<>'' AND t.conversation_id=? AND t.provider_turn_id=? AND t.provider_turn_id<>'' AND t.state NOT IN ('queued','running')
 UNION ALL SELECT 1 FROM conversation_messages m JOIN conversation_turns t ON t.id=m.turn_id JOIN ao_history_retention r ON r.object_kind='message' AND r.object_id=m.id WHERE ?<>'' AND m.conversation_id=? AND m.provider_item_id=? AND m.provider_item_id<>'' AND t.state NOT IN ('queued','running')
 UNION ALL SELECT 1 FROM conversation_messages m JOIN conversation_turns t ON t.id=m.turn_id JOIN ao_history_retention r ON r.object_kind='message' AND r.object_id=m.id WHERE ?<>'' AND m.conversation_id=? AND m.client_message_id=? AND m.client_message_id<>'' AND t.state NOT IN ('queued','running')
 UNION ALL SELECT 1 FROM conversation_activities a JOIN conversation_turns t ON t.id=a.turn_id JOIN ao_history_retention r ON r.object_kind='activity' AND r.object_id=a.id WHERE ?<>'' AND a.conversation_id=? AND a.provider_item_id=? AND a.provider_item_id<>'' AND t.state NOT IN ('queued','running')
 )`

func redactExpiredProviderPayload(ctx context.Context, db gen.DBTX, conversationID, payload string) (string, bool, error) {
	var body map[string]json.RawMessage
	if json.Unmarshal([]byte(payload), &body) != nil {
		return payload, false, nil
	}
	var providerTurnID, itemID, clientID string
	for key, target := range map[string]*string{"providerTurnId": &providerTurnID, "providerItemId": &itemID, "clientMessageId": &clientID} {
		if value, ok := body[key]; ok {
			if json.Unmarshal(value, target) != nil {
				return payload, false, nil
			}
		}
	}
	if providerTurnID == "" && itemID == "" && clientID == "" {
		return payload, false, nil
	}
	var expired bool
	err := db.QueryRowContext(ctx, expiredProviderPayloadSQL, providerTurnID, conversationID, providerTurnID, itemID, conversationID, itemID, clientID, conversationID, clientID, itemID, conversationID, itemID).Scan(&expired)
	if err != nil {
		return "", false, err
	}
	if !expired {
		return payload, false, nil
	}
	for _, key := range []string{"delta", "text", "summary", "detail", "diff", "input", "plan"} {
		if _, ok := body[key]; ok {
			delete(body, key)
		}
	}
	body["_retention"] = json.RawMessage(`{"contentExpired":true,"protocol":1}`)
	encoded, err := json.Marshal(body)
	return string(encoded), true, err
}

// HistoryRetentionCapability is advertised only by this compatible running
// binary, after schema initialization. File identity binds the caller's target.
func (s *Store) HistoryRetentionCapability() map[string]any {
	return s.historyRetentionCapability(context.Background())
}

func (s *Store) historyRetentionCapability(ctx context.Context) map[string]any {
	result := map[string]any{"protocol": 1, "schemaReady": false}
	var seq int
	var name, path string
	if err := s.readDB.QueryRowContext(ctx, `PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		return result
	}
	info, err := os.Stat(path)
	if err != nil {
		return result
	}
	sys := reflect.ValueOf(info.Sys())
	if sys.Kind() == reflect.Pointer {
		sys = sys.Elem()
	}
	if sys.Kind() != reflect.Struct {
		return result
	}
	dev, ino := sys.FieldByName("Dev"), sys.FieldByName("Ino")
	if !dev.IsValid() || !ino.IsValid() || !dev.CanUint() || !ino.CanUint() {
		return result
	}
	if err = validateHistoryRetentionSchema(ctx, s.readDB); err != nil {
		return result
	}
	statData, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return result
	}
	end := strings.LastIndex(string(statData), ")")
	if end < 0 {
		return result
	}
	fields := strings.Fields(string(statData)[end+1:])
	if len(fields) <= 19 {
		return result
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return result
	}
	result["processStartTicks"] = ticks
	result["schemaReady"] = true
	result["dbDevice"] = dev.Uint()
	result["dbInode"] = ino.Uint()
	return result
}

// Legacy sources remain mode=ro. Adapt only this fixed generated SELECT
// expression when the optional sidecar is absent; no source/TEMP schema writes.
const sessionPromptExpiryExpression = "EXISTS(SELECT 1 FROM ao_history_retention WHERE object_kind='session' AND object_id=sessions.id) AS prompt_expired"

type legacyHistoryReader struct{ gen.DBTX }

func (r legacyHistoryReader) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return r.DBTX.QueryContext(ctx, strings.ReplaceAll(query, sessionPromptExpiryExpression, "0 AS prompt_expired"), args...)
}
func (r legacyHistoryReader) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return r.DBTX.QueryRowContext(ctx, strings.ReplaceAll(query, sessionPromptExpiryExpression, "0 AS prompt_expired"), args...)
}

func NewReadOnlyStore(ctx context.Context, writeDB, readDB *sql.DB) (*Store, error) {
	var present bool
	if err := readDB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='ao_history_retention')`).Scan(&present); err != nil {
		return nil, err
	}
	s := NewStore(writeDB, readDB)
	s.readOnly = true
	s.historyRetentionAbsent = !present
	if !present {
		s.qr = gen.New(legacyHistoryReader{readDB})
	}
	return s, nil
}
func (s *Store) historyReadQueries(tx *sql.Tx) *gen.Queries {
	if s.historyRetentionAbsent {
		return gen.New(legacyHistoryReader{tx})
	}
	return s.qr.WithTx(tx)
}

func (s *Store) beginConversationSnapshot(ctx context.Context, conversationID string) (*sql.Tx, *gen.Queries, gen.Conversation, error) {
	// Release the preliminary read before any lazy reconciliation writer is taken.
	conv, err := s.qr.SelectConversationByID(ctx, conversationID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, conv, ErrConversationNotFound
	}
	if err != nil {
		return nil, nil, conv, err
	}
	if !s.readOnly {
		if account := conversationToDomain(conv).Account; account != nil && account.ReauthRequiredAt != nil {
			if _, err = s.ReconcileConversationAuthentication(ctx, conversationID, "", "", time.Now()); err != nil {
				return nil, nil, conv, err
			}
		}
	}
	tx, err := s.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, conv, err
	}
	q := s.historyReadQueries(tx)
	conv, err = q.SelectConversationByID(ctx, conversationID)
	if err != nil {
		tx.Rollback()
		if errors.Is(err, sql.ErrNoRows) {
			err = ErrConversationNotFound
		}
		return nil, nil, conv, err
	}
	return tx, q, conv, nil
}
func (s *Store) historyTurnExpired(ctx context.Context, db gen.DBTX, conversationID, turnID string) (bool, error) {
	if s.historyRetentionAbsent {
		return false, nil
	}
	var expired bool
	err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM ao_history_retention r WHERE r.object_kind='turn' AND r.object_id=? AND r.conversation_id=?
 UNION ALL SELECT 1 FROM conversation_messages m JOIN ao_history_retention r ON r.object_kind='message' AND r.object_id=m.id WHERE m.conversation_id=? AND m.turn_id=? AND m.role='user')`, turnID, conversationID, conversationID, turnID).Scan(&expired)
	return expired, err
}

func validateHistoryRetentionSchema(ctx context.Context, db gen.DBTX) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(ao_history_retention)`)
	if err != nil {
		return err
	}
	want := []string{"object_kind", "object_id", "conversation_id", "semantic_sha256", "prompt_sha256", "original_bytes", "original_sha256", "expired_at", "cutoff_at", "fields_json"}
	i := 0
	for rows.Next() {
		var cid, nn, pk int
		var name, typ string
		var def any
		if err = rows.Scan(&cid, &name, &typ, &nn, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		expectedPK := 0
		if i < 2 {
			expectedPK = i + 1
		}
		expectedType := "TEXT"
		if i == 5 {
			expectedType = "INTEGER"
		}
		if i >= len(want) || name != want[i] || typ != expectedType || nn != 1 || pk != expectedPK {
			rows.Close()
			return fmt.Errorf("incompatible history retention schema column %d", i)
		}
		i++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if i != len(want) {
		return errors.New("incomplete history retention schema")
	}

	return nil
}

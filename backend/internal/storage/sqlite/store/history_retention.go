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
	rows, err := tx.Query(`PRAGMA table_info(ao_history_retention)`)
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
	rows, err := db.QueryContext(ctx, `SELECT object_kind,object_id,semantic_sha256,prompt_sha256 FROM ao_history_retention WHERE conversation_id=?`, conversationID)
	if err != nil {
		return err
	}
	defer rows.Close()
	type marker struct{ semantic, prompt string }
	markers := map[string]marker{}
	for rows.Next() {
		var kind, id string
		var m marker
		if err = rows.Scan(&kind, &id, &m.semantic, &m.prompt); err != nil {
			return err
		}
		markers[kind+"\x00"+id] = m
	}
	if err = rows.Err(); err != nil {
		return err
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
func redactExpiredProviderPayload(ctx context.Context, db gen.DBTX, conversationID, payload string) (string, bool, error) {
	var body map[string]json.RawMessage
	if json.Unmarshal([]byte(payload), &body) != nil {
		return payload, false, nil
	}
 var providerTurnID,itemID,clientID string
 for key,target:=range map[string]*string{"providerTurnId":&providerTurnID,"providerItemId":&itemID,"clientMessageId":&clientID} {if value,ok:=body[key];ok {if json.Unmarshal(value,target)!=nil{return payload,false,nil}}}
 if providerTurnID=="" && itemID=="" && clientID=="" {return payload,false,nil}
 var expired bool
 err:=db.QueryRowContext(ctx,`SELECT EXISTS(
 SELECT 1 FROM conversation_turns t JOIN ao_history_retention r ON r.object_kind='turn' AND r.object_id=t.id WHERE ?<>'' AND t.conversation_id=? AND t.provider_turn_id=? AND t.state NOT IN ('queued','running')
 UNION ALL SELECT 1 FROM conversation_messages m JOIN conversation_turns t ON t.id=m.turn_id JOIN ao_history_retention r ON r.object_kind='message' AND r.object_id=m.id WHERE ?<>'' AND m.conversation_id=? AND m.provider_item_id=? AND t.state NOT IN ('queued','running')
 UNION ALL SELECT 1 FROM conversation_messages m JOIN conversation_turns t ON t.id=m.turn_id JOIN ao_history_retention r ON r.object_kind='message' AND r.object_id=m.id WHERE ?<>'' AND m.conversation_id=? AND m.client_message_id=? AND t.state NOT IN ('queued','running')
 UNION ALL SELECT 1 FROM conversation_activities a JOIN conversation_turns t ON t.id=a.turn_id JOIN ao_history_retention r ON r.object_kind='activity' AND r.object_id=a.id WHERE ?<>'' AND a.conversation_id=? AND a.provider_item_id=? AND t.state NOT IN ('queued','running')
 )`,providerTurnID,conversationID,providerTurnID,itemID,conversationID,itemID,clientID,conversationID,clientID,itemID,conversationID,itemID).Scan(&expired)
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
	result := map[string]any{"protocol": 1, "schemaReady": false}
	var seq int
	var name, path string
	if err := s.readDB.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
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
	var columns int
	if err = s.readDB.QueryRow(`SELECT count(*) FROM pragma_table_info('ao_history_retention')`).Scan(&columns); err != nil || columns != 10 {
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

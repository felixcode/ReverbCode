package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func maintenancePrepareFixture(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ao.db")
	w, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		t.Fatal(err)
	}
	w.SetMaxOpenConns(1)
	r, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		w.Close()
		t.Fatal(err)
	}
	r.SetMaxOpenConns(1)
	t.Cleanup(func() { r.Close(); w.Close() })
	_, err = w.Exec(`CREATE TABLE goose_db_version(version_id INTEGER,is_applied INTEGER);INSERT INTO goose_db_version VALUES(191,1);
 CREATE TABLE sessions(id TEXT PRIMARY KEY,is_terminated INTEGER,prompt TEXT,updated_at TEXT);INSERT INTO sessions VALUES('owner',0,'initial','2020-01-01T00:00:00Z');
 CREATE TABLE conversations(id TEXT PRIMARY KEY,scope TEXT,session_id TEXT,current_session_id TEXT);INSERT INTO conversations VALUES('conv','session','owner','owner');
 CREATE TABLE conversation_branches(session_id TEXT,conversation_id TEXT);INSERT INTO conversation_branches VALUES('owner','conv');
 CREATE TABLE conversation_turns(id TEXT PRIMARY KEY,conversation_id TEXT,handled_by_session_id TEXT,provider_turn_id TEXT,state TEXT,completed_at TEXT,branch_id TEXT,diff_json TEXT,plan_json TEXT,error_message TEXT);
 INSERT INTO conversation_turns VALUES('turn','conv','owner','native-old','completed','2020-01-01T00:00:00Z','branch','','','');
 CREATE UNIQUE INDEX native_turn ON conversation_turns(conversation_id,provider_turn_id) WHERE provider_turn_id<>'';
 CREATE TABLE conversation_messages(id TEXT PRIMARY KEY,conversation_id TEXT,turn_id TEXT,role TEXT,streaming INTEGER,revision INTEGER,branch_id TEXT,created_at TEXT,text TEXT,delivery_content_json TEXT);
 INSERT INTO conversation_messages VALUES('message','conv','turn','user',0,1,'branch','2020-01-01T00:00:00Z','body-old','');
 CREATE TABLE conversation_activities(id TEXT PRIMARY KEY,conversation_id TEXT,turn_id TEXT,kind TEXT,status TEXT,revision INTEGER,branch_id TEXT,created_at TEXT,summary TEXT,detail_json TEXT,command_output TEXT,streamed_text TEXT,command_output_truncated INTEGER,streamed_text_truncated INTEGER);
 INSERT INTO conversation_activities VALUES('activity','conv','turn','command','completed',1,'branch','2020-01-01T00:00:00Z','summary','{}','x','',0,0);
 CREATE TABLE conversation_provider_events(id INTEGER PRIMARY KEY,conversation_id TEXT,session_id TEXT,payload_json TEXT,received_at TEXT);
 INSERT INTO conversation_provider_events VALUES(1,'conv','owner','{"providerTurnId":"native-old","text":"body-old"}','2020-01-01T00:00:00Z');
 CREATE TABLE agent_switches(session_id TEXT,state TEXT);CREATE TABLE review_run(session_id TEXT,status TEXT);
 CREATE TABLE telemetry_event(id TEXT PRIMARY KEY,occurred_at TEXT,payload_json TEXT);INSERT INTO telemetry_event VALUES('event','2020-01-01T00:00:00Z','old');
 CREATE TABLE business(id INTEGER PRIMARY KEY);
 ALTER TABLE conversation_turns ADD COLUMN controller_generation TEXT NOT NULL DEFAULT '';
 ALTER TABLE conversation_provider_events ADD COLUMN provider_event_id TEXT NOT NULL DEFAULT '';
 ALTER TABLE conversation_provider_events ADD COLUMN method TEXT NOT NULL DEFAULT 'message.completed';
 ALTER TABLE conversation_messages ADD COLUMN provider_item_id TEXT NOT NULL DEFAULT '';
 ALTER TABLE conversation_messages ADD COLUMN client_message_id TEXT NOT NULL DEFAULT '';
 ALTER TABLE conversation_messages ADD COLUMN client_payload_hash TEXT NOT NULL DEFAULT '';
 ALTER TABLE conversation_activities ADD COLUMN provider_item_id TEXT NOT NULL DEFAULT '';
 ALTER TABLE conversation_activities ADD COLUMN request_id TEXT NOT NULL DEFAULT '';`)
	if err != nil {
		t.Fatal(err)
	}
	if err = EnsureHistoryRetentionSchema(w); err != nil {
		t.Fatal(err)
	}
	return w, r
}
func TestMaintenancePreparationCASRejectsBodyOwnerRevisionAndBranchChanges(t *testing.T) {
	cases := []struct {
		name, stage, key, change string
		id                       any
	}{
		{"raw_same_size_body", "raw", "id", `UPDATE conversation_provider_events SET payload_json='{"providerTurnId":"native-new","text":"body-new"}' WHERE id=1`, int64(1)},
		{"turn_became_running", "message", "id", `UPDATE conversation_turns SET state='running' WHERE id='turn'`, "message"},
		{"revision", "message", "id", `UPDATE conversation_messages SET revision=revision+1 WHERE id='message'`, "message"},
		{"branch", "message", "id", `UPDATE conversation_messages SET branch_id='other' WHERE id='message'`, "message"},
		{"builtin_same_size_body", "builtin:telemetry_event", "id", `UPDATE telemetry_event SET payload_json='new' WHERE id='event'`, "event"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, r := maintenancePrepareFixture(t)
			ctx := context.Background()
			o := DefaultHistoryMaintenanceOptions()
			now := time.Now().UTC()
			cutoff := now.Add(-48 * time.Hour)
			plans, err := prepareMaintenanceRows(ctx, r, maintenanceStages[tc.stage], tc.stage, tc.key, []any{tc.id}, o, now, cutoff)
			if err != nil || len(plans) != 1 || plans[0].skip != "" {
				t.Fatal("prepare", len(plans), err)
			}
			if _, err = w.Exec(tc.change); err != nil {
				t.Fatal(err)
			}
			tx, err := w.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			result := HistoryMaintenanceResult{Retained: map[string]int{}, NextCursor: "previous"}
			advanced, err := applyMaintenancePrepared(ctx, tx, maintenanceStages[tc.stage], tc.stage, tc.key, plans[0], cutoff, now, &result)
			if err != nil || advanced || result.Compacted != 0 || result.Deleted != 0 || result.Markers != 0 || result.NextCursor != "previous" {
				tx.Rollback()
				t.Fatal("stale preparation applied", result, err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			var markers int
			if err = r.QueryRow(`SELECT count(*) FROM ao_history_retention`).Scan(&markers); err != nil || markers != 0 {
				t.Fatal("stale marker", err)
			}
		})
	}
}

type maintenanceObservedDB struct {
	db      *sql.DB
	queries *[]string
}

func (o maintenanceObservedDB) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	*o.queries = append(*o.queries, q)
	return o.db.QueryContext(ctx, q, args...)
}
func (o maintenanceObservedDB) QueryRowContext(ctx context.Context, q string, args ...any) *sql.Row {
	*o.queries = append(*o.queries, q)
	return o.db.QueryRowContext(ctx, q, args...)
}
func TestMaintenanceOutputsPrepareNeverLoadsUnrelatedLargeDetails(t *testing.T) {
	w, r := maintenancePrepareFixture(t)
	ctx := context.Background()
	huge := strings.Repeat("x", 32<<20)
	if _, err := w.Exec(`UPDATE conversation_activities SET detail_json=? WHERE id='activity'`, huge); err != nil {
		t.Fatal(err)
	}
	queries := []string{}
	o := DefaultHistoryMaintenanceOptions()
	o.MaxRowBytes = 8
	o.BatchBytes = 16
	now := time.Now().UTC()
	cutoff := now.Add(-48 * time.Hour)
	plans, err := prepareMaintenanceRows(ctx, maintenanceObservedDB{r, &queries}, maintenanceStages["outputs"], "outputs", "id", []any{"activity"}, o, now, cutoff)
	if err != nil || len(plans) != 1 || plans[0].skip != "" || plans[0].size != 1 {
		t.Fatal("small output rejected", err)
	}
	for _, q := range queries {
		if strings.Contains(q, "detail_json") || strings.Contains(q, "summary") {
			t.Fatal("unrelated body selected during prepare")
		}
	}
	if _, ok := plans[0].row["detail_json"]; ok {
		t.Fatal("large detail materialized")
	}
	tx, err := w.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := HistoryMaintenanceResult{Retained: map[string]int{}}
	advanced, err := applyMaintenancePrepared(ctx, tx, maintenanceStages["outputs"], "outputs", "id", plans[0], cutoff, now, &result)
	if err != nil || !advanced || result.Compacted != 1 || result.LogicalBytes != 1 {
		tx.Rollback()
		t.Fatal(result, err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var remaining, size int
	if err = r.QueryRow(`SELECT octet_length(command_output),octet_length(detail_json) FROM conversation_activities WHERE id='activity'`).Scan(&remaining, &size); err != nil || remaining != 0 || size != len(huge) {
		t.Fatal("unrelated detail changed", err)
	}
}
func TestMaintenanceSinglePrepareDoesNotReserveBusinessWriter(t *testing.T) {
	w, r := maintenancePrepareFixture(t)
	s := NewStore(w, r)
	ctx := context.Background()
	cap := s.HistoryRetentionCapability()
	req := HistoryMaintenanceRequest{Stage: "raw", Cutoff: time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339Nano), DatabaseDevice: cap["dbDevice"].(uint64), DatabaseInode: cap["dbInode"].(uint64)}
	held, err := r.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { _, e := s.MaintainHistoryBatch(ctx, req, DefaultHistoryMaintenanceOptions()); finished <- e }()
	deadline := time.Now().Add(time.Second)
	for !s.maintenanceBusy.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	second, err := s.MaintainHistoryBatch(ctx, req, DefaultHistoryMaintenanceOptions())
	if err != nil || second.Status != "deferred" || second.Retained["maintenance_busy"] != 1 {
		held.Close()
		t.Fatal("parallel preparation admitted", second, err)
	}
	businessCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err = s.writeMu.LockContext(businessCtx); err != nil {
		held.Close()
		t.Fatal("prepare reserved business mutex", err)
	}
	_, err = w.ExecContext(businessCtx, `INSERT INTO business VALUES(1)`)
	s.writeMu.Unlock()
	if err != nil {
		held.Close()
		t.Fatal("prepare blocked writer", err)
	}
	held.Close()
	if err = <-finished; err != nil {
		t.Fatal(err)
	}
	if s.maintenanceBusy.Load() {
		t.Fatal("maintenance admission leaked")
	}
}

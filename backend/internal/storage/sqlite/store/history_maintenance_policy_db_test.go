package store

import (
	"context"
	"database/sql"
	_ "modernc.org/sqlite"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMaintenanceClosedPathsProtectHandoffAndReview(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "policy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.Exec(`CREATE TABLE sessions(id TEXT PRIMARY KEY,is_terminated INTEGER);INSERT INTO sessions VALUES('owner',1);
 CREATE TABLE conversations(id TEXT PRIMARY KEY,scope TEXT,session_id TEXT,current_session_id TEXT);INSERT INTO conversations VALUES('conv','session','owner','owner');
 CREATE TABLE conversation_branches(session_id TEXT,conversation_id TEXT);INSERT INTO conversation_branches VALUES('owner','conv');
 CREATE TABLE conversation_turns(id TEXT,conversation_id TEXT,handled_by_session_id TEXT,state TEXT,completed_at TEXT);
 CREATE TABLE conversation_activities(conversation_id TEXT,turn_id TEXT,kind TEXT,status TEXT);
 CREATE TABLE agent_switches(session_id TEXT,state TEXT);CREATE TABLE review_run(session_id TEXT,status TEXT);`)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-72 * time.Hour).UTC().Format(time.RFC3339Nano)
	cutoff := time.Now().Add(-48 * time.Hour)
	raw := maintenanceRow{"conversation_id": "conv", "session_id": "owner", "received_at": old, "payload_json": `{"text":"UNIQUE-EXPIRED-SENTINEL"}`}
	message := maintenanceRow{"conversation_id": "conv", "created_at": old, "text": "UNIQUE-EXPIRED-SENTINEL"}
	for _, blocked := range []string{"handoff", "review"} {
		table := "agent_switches"
		if blocked == "review" {
			table = "review_run"
		}
		column := "state"
		if blocked == "review" {
			column = "status"
		}
		phase := "running"
		if blocked == "handoff" {
			phase = "preparing_handoff"
		}
		if _, err = db.Exec(`INSERT INTO `+table+` VALUES('owner',?)`, phase); err != nil {
			t.Fatal(err)
		}
		closed, e := maintenanceClosedOwner(ctx, db, "conv")
		if e != nil || closed {
			t.Fatal("closed proof bypassed", blocked, e)
		}
		for stage, row := range map[string]maintenanceRow{"raw": raw, "message": message} {
			ok, _ := maintenanceEligible(ctx, db, row, stage, cutoff)
			if ok {
				t.Fatal("body cleared across active authority", stage, blocked)
			}
		}
		if _, err = db.Exec(`UPDATE ` + table + ` SET ` + column + `='completed'`); err != nil {
			t.Fatal(err)
		}
	}
	closed, err := maintenanceClosedOwner(ctx, db, "conv")
	if err != nil || !closed {
		t.Fatal("settled owner not recognized", err)
	}
}

func TestMaintenanceAllBuiltinPredicatesKeepUnprovenAndPending(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "builtin.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.Exec(`CREATE TABLE sessions(id TEXT PRIMARY KEY,is_terminated INTEGER,activity_state TEXT);INSERT INTO sessions VALUES('closed',1,'idle'),('active',0,'waiting_input');
 CREATE TABLE change_log(seq INTEGER PRIMARY KEY,created_at TEXT);CREATE TABLE telemetry_event(id TEXT PRIMARY KEY,occurred_at TEXT);
 CREATE TABLE agent_switch_failure_outbox(id TEXT PRIMARY KEY,expires_at TEXT);CREATE TABLE agent_switch_failure_receipts(id TEXT PRIMARY KEY,retain_until TEXT);
 CREATE TABLE notifications(id TEXT PRIMARY KEY,dismissed_at TEXT,resolved_at TEXT,type TEXT,session_id TEXT);CREATE TABLE model_usage_events(id INTEGER PRIMARY KEY,amount INTEGER);INSERT INTO model_usage_events VALUES(1,12345);`)
	if err != nil {
		t.Fatal(err)
	}
	cutoff := time.Date(2026, 10, 24, 0, 0, 0, 123456789, time.UTC)
	now := cutoff.Add(48 * time.Hour)
	for _, stage := range []string{"builtin:change_log", "builtin:telemetry_event", "builtin:agent_switch_failure_outbox", "builtin:agent_switch_failure_receipts", "builtin:notifications"} {
		plan := maintenanceStages[stage]
		key := "id"
		column := "created_at"
		switch plan.table {
		case "change_log":
			key = "seq"
		case "telemetry_event":
			column = "occurred_at"
		case "agent_switch_failure_outbox":
			column = "expires_at"
		case "agent_switch_failure_receipts":
			column = "retain_until"
		case "notifications":
			column = "resolved_at"
		}
		ttl := plan.table == "agent_switch_failure_outbox" || plan.table == "agent_switch_failure_receipts"
		boundary := cutoff
		if ttl {
			boundary = now
		}
		stamps := []string{boundary.Add(-time.Nanosecond).Format(time.RFC3339Nano), boundary.Format(time.RFC3339Nano), boundary.Add(time.Nanosecond).Format(time.RFC3339Nano), "2026-10-24T00:00:00+00:60"}
		for i, stamp := range stamps {
			if plan.table == "notifications" {
				_, err = db.Exec(`INSERT INTO notifications VALUES(?,?,?,?,?)`, i+1, stamp, stamp, "notice", "closed")
			} else {
				_, err = db.Exec(`INSERT INTO `+plan.table+`("`+key+`",`+column+`)VALUES(?,?)`, i+1, stamp)
			}
			if err != nil {
				t.Fatal(err)
			}
			tx, e := db.BeginTx(ctx, nil)
			if e != nil {
				t.Fatal(e)
			}
			deleted, e := maintenanceBuiltin(ctx, tx, plan, key, i+1, cutoff, now)
			if i == 3 {
				if e == nil || deleted {
					t.Fatal("malformed age was accepted", stage)
				}
				tx.Rollback()
				continue
			}
			want := i == 0 || ttl && i == 1
			if e != nil || deleted != want {
				tx.Rollback()
				t.Fatal("builtin boundary", stage, i, deleted, e)
			}
			if e = tx.Commit(); e != nil {
				t.Fatal(e)
			}
		}
	}
	if _, err = db.Exec(`INSERT INTO notifications VALUES('pending','2020-01-01T00:00:00Z',NULL,'needs_input','active')`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := maintenanceBuiltin(ctx, tx, maintenanceStages["builtin:notifications"], "id", "pending", cutoff, now)
	if err != nil || deleted {
		t.Fatal("pending request deleted", err)
	}
	tx.Commit()
	var amount int
	if err = db.QueryRow(`SELECT amount FROM model_usage_events WHERE id=1`).Scan(&amount); err != nil || amount != 12345 {
		t.Fatal("billing changed", err)
	}
}

func TestMaintenanceMarkerSchemaRequiresExactColumns(t *testing.T) {
	for _, variant := range []string{"valid", "reordered", "wrong_type"} {
		t.Run(variant, func(t *testing.T) {
			db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "schema.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			definitions := []string{"object_kind TEXT NOT NULL", "object_id TEXT NOT NULL", "conversation_id TEXT NOT NULL", "semantic_sha256 TEXT NOT NULL", "prompt_sha256 TEXT NOT NULL", "original_bytes INTEGER NOT NULL", "original_sha256 TEXT NOT NULL", "expired_at TEXT NOT NULL", "cutoff_at TEXT NOT NULL", "fields_json TEXT NOT NULL"}
			if variant == "reordered" {
				definitions[2], definitions[3] = definitions[3], definitions[2]
			}
			if variant == "wrong_type" {
				definitions[5] = "original_bytes TEXT NOT NULL"
			}
			if _, err = db.Exec(`CREATE TABLE ao_history_retention (` + strings.Join(definitions, ",") + `,PRIMARY KEY(object_kind,object_id))`); err != nil {
				t.Fatal(err)
			}
			err = validateHistoryRetentionSchema(context.Background(), db)
			if (err == nil) != (variant == "valid") {
				t.Fatal("schema drift accepted", variant, err)
			}
		})
	}
}

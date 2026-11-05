package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func seedAlbanewsLegacy0148(t *testing.T, partial bool) *sql.DB {
	t.Helper()
	db := openMigratedDatabaseCopy(t, 190)
	for _, name := range []string{"0191_provider_context.sql", "0192_review_run_running_session.sql"} {
		body, err := os.ReadFile("compatibility/" + name)
		if err != nil {
			t.Fatal(err)
		}
		up, err := migrationUp(body)
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, db, up)
	}
	mustExec(t, db, `INSERT INTO goose_db_version(version_id,is_applied) VALUES(191,1),(192,1)`)
	if partial {
		body, err := migrationsFS.ReadFile("migrations/0191_session_branch_state.sql")
		if err != nil {
			t.Fatal(err)
		}
		up, err := migrationUp(body)
		if err != nil {
			t.Fatal(err)
		}
		mustExec(t, db, up)
	}
	return db
}

func TestAlbanews0148LegacyBridgeRunsBeforeOfficialMigrations(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "legacy"
		if partial {
			name = "official191_already_repaired"
		}
		t.Run(name, func(t *testing.T) {
			db := seedAlbanewsLegacy0148(t, partial)
			mustExec(t, db, `INSERT INTO projects(id,path,registered_at) VALUES('preserved-project','/synthetic',CURRENT_TIMESTAMP);
INSERT INTO sessions(id,project_id,num,harness,session_mode,provider_context,activity_last_at,created_at,updated_at)
VALUES('preserved-worker','preserved-project',1,'codex','chat','{"profile":"synthetic-private"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
INSERT INTO conversations(id,scope,project_id,session_id,current_session_id,active_branch_id,created_at,updated_at)
VALUES('preserved-conversation','session','preserved-project','preserved-worker','preserved-worker','preserved-root',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);
INSERT INTO conversation_branches(id,conversation_id,session_id,provider_conversation_id,created_at)
VALUES('preserved-root','preserved-conversation','preserved-worker','same-native-thread',CURRENT_TIMESTAMP);
INSERT INTO conversation_turns(id,conversation_id,handled_by_session_id,state,requested_at,branch_id)
VALUES('preserved-queued-turn','preserved-conversation','preserved-worker','queued',CURRENT_TIMESTAMP,'preserved-root');
INSERT INTO conversation_messages(id,conversation_id,turn_id,sequence,role,origin,text,created_at,updated_at,branch_id)
VALUES('preserved-input','preserved-conversation','preserved-queued-turn',1,'user','human','synthetic queued prompt',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'preserved-root');`)
			var rowid, ledgerBefore int64
			if err := db.QueryRow(`SELECT rowid FROM sessions WHERE id='preserved-worker'`).Scan(&rowid); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT count(*) FROM goose_db_version WHERE version_id IN (191,192)`).Scan(&ledgerBefore); err != nil {
				t.Fatal(err)
			}
			if err := migrate(db); err != nil {
				t.Fatal(err)
			}
			if err := prepareAlbanews0148Bridge(db); err != nil {
				t.Fatalf("repeated bridge: %v", err)
			}
			var afterID, ledgerAfter int64
			var contextValue, turnState, prompt, session string
			if err := db.QueryRow(`SELECT rowid,provider_context FROM sessions WHERE id='preserved-worker'`).Scan(&afterID, &contextValue); err != nil {
				t.Fatal(err)
			}
			if rowid != afterID || contextValue != "{\"profile\":\"synthetic-private\"}" {
				t.Fatal("provider or session identity changed")
			}
			if err := db.QueryRow(`SELECT count(*) FROM goose_db_version WHERE version_id IN (191,192)`).Scan(&ledgerAfter); err != nil {
				t.Fatal(err)
			}
			if ledgerAfter != ledgerBefore {
				t.Fatal("old migration ledger changed")
			}
			if err := db.QueryRow(`SELECT state FROM conversation_turns WHERE id='preserved-queued-turn'`).Scan(&turnState); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT text FROM conversation_messages WHERE id='preserved-input'`).Scan(&prompt); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow(`SELECT current_session_id FROM conversations WHERE id='preserved-conversation'`).Scan(&session); err != nil {
				t.Fatal(err)
			}
			if turnState != "queued" || prompt != "synthetic queued prompt" || session != "preserved-worker" {
				t.Fatal("queued intake semantics changed")
			}
			var indexCount int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='idx_review_run_running_session'`).Scan(&indexCount); err != nil || indexCount != 1 {
				t.Fatal("local running-review index lost")
			}
			var check string
			if err := db.QueryRow(`PRAGMA quick_check`).Scan(&check); err != nil || check != "ok" {
				t.Fatal("fixture integrity failed")
			}
		})
	}
}

func TestAlbanews0148UnknownCollisionRefusesWithoutLedgerEdits(t *testing.T) {
	db := openMigratedDatabaseCopy(t, 190)
	mustExec(t, db, `INSERT INTO goose_db_version(version_id,is_applied) VALUES(191,1),(192,1)`)
	if err := prepareAlbanews0148Bridge(db); err == nil || !strings.Contains(err.Error(), "shape_unknown") {
		t.Fatal("unknown collision accepted")
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM goose_db_version WHERE version_id IN (191,192) AND is_applied=1`).Scan(&n); err != nil || n != 2 {
		t.Fatal("old ledger modified")
	}
}

func TestAlbanews0148BranchColumnWithoutCDCRefuses(t *testing.T) {
	db := seedAlbanewsLegacy0148(t, false)
	mustExec(t, db, `ALTER TABLE sessions ADD COLUMN branch_state TEXT NOT NULL DEFAULT ''`)
	if err := prepareAlbanews0148Bridge(db); err == nil || !strings.Contains(err.Error(), "branch_trigger_shape_unknown") {
		t.Fatal("partial branch schema treated complete")
	}
}

func TestAlbanews0148FreshOfficialSchemaGetsLocalProviderColumns(t *testing.T) {
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db, err := sql.Open("sqlite", filepath.Join(dir, "ao.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"sessions", "review"} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM pragma_table_info(?) WHERE name='provider_context' AND type='TEXT' AND "notnull"=1`, table).Scan(&n); err != nil || n != 1 {
			t.Fatal("fresh provider schema missing")
		}
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name='idx_review_run_running_session'`).Scan(&n); err != nil || n != 1 {
		t.Fatal("fresh local index missing")
	}
}

func TestUpgradeFailureDetailsWhitelist(t *testing.T) {
	stage, version := UpgradeFailureDetails(migrationFailure("official_goose", fmt.Errorf("ERROR 20261028124744441_telemetry_incremental_vacuum.sql: PRIVATE SQL/DSN")))
	if stage != "official_goose" || version != "20261028124744441" {
		t.Fatal("safe migration details missing")
	}
	stage, version = UpgradeFailureDetails(migrationFailure("untrusted/path", fmt.Errorf("PRIVATE SQL 12345_fake.sql")))
	if stage != "" || version != "" {
		t.Fatal("untrusted stage escaped")
	}
	stage, version = UpgradeFailureDetails(migrationFailure("bridge", fmt.Errorf("PRIVATE SQL 12345_fake.sql")))
	if stage != "bridge" || version != "" {
		t.Fatal("untrusted migration escaped")
	}
}

//go:build linux

package historycopy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/maintenance/rawcopy"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

var testAsOf = time.Date(2026, 10, 25, 21, 28, 16, 835298000, time.UTC)

func fixture(t *testing.T) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sealed.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE goose_db_version(version_id INTEGER,is_applied INTEGER);INSERT INTO goose_db_version VALUES(191,1);
CREATE TABLE sessions(id TEXT PRIMARY KEY,is_terminated INTEGER,prompt TEXT,updated_at TEXT,activity_state TEXT);
INSERT INTO sessions VALUES('owner',0,'PROTECTED-SESSION','2020-01-01T00:00:00Z','idle'),('retired',1,'EXPIRED-SESSION','2020-01-01T00:00:00Z','exited');
CREATE TABLE conversations(id TEXT PRIMARY KEY,scope TEXT,session_id TEXT,current_session_id TEXT);INSERT INTO conversations VALUES('conv','session','owner','owner');
CREATE TABLE conversation_branches(session_id TEXT,conversation_id TEXT);INSERT INTO conversation_branches VALUES('owner','conv');
CREATE TABLE conversation_turns(id TEXT PRIMARY KEY,conversation_id TEXT,handled_by_session_id TEXT,provider_turn_id TEXT,state TEXT,completed_at TEXT,branch_id TEXT,diff_json TEXT,plan_json TEXT,error_message TEXT,controller_generation TEXT);
INSERT INTO conversation_turns(rowid,id,conversation_id,handled_by_session_id,provider_turn_id,state,completed_at,branch_id,diff_json,plan_json,error_message,controller_generation)VALUES(9,'old','conv','owner','native-old','completed','2020-01-01T00:00:00Z','branch','EXPIRED-DIFF','','','gen'),(21,'busy','conv','owner','native-busy','running',NULL,'branch','PROTECTED-DIFF','','','gen');
CREATE UNIQUE INDEX native_turn ON conversation_turns(conversation_id,provider_turn_id) WHERE provider_turn_id<>'';
CREATE TABLE conversation_messages(id TEXT PRIMARY KEY,conversation_id TEXT,turn_id TEXT,role TEXT,streaming INTEGER,revision INTEGER,branch_id TEXT,created_at TEXT,text TEXT,delivery_content_json TEXT,provider_item_id TEXT,client_message_id TEXT,client_payload_hash TEXT);
INSERT INTO conversation_messages VALUES('old-message','conv','old','user',0,1,'branch','2020-01-01T00:00:00Z','EXPIRED-MESSAGE','','item-old','client-old','payload-old'),('busy-message','conv','busy','user',0,1,'branch','2020-01-01T00:00:00Z','PROTECTED-MESSAGE','','item-busy','client-busy','payload-busy');
CREATE TABLE conversation_activities(id TEXT PRIMARY KEY,conversation_id TEXT,turn_id TEXT,kind TEXT,status TEXT,revision INTEGER,branch_id TEXT,created_at TEXT,summary TEXT,detail_json TEXT,command_output TEXT,streamed_text TEXT,command_output_truncated INTEGER,streamed_text_truncated INTEGER,provider_item_id TEXT,request_id TEXT);
INSERT INTO conversation_activities VALUES('old-activity','conv','old','command','completed',1,'branch','2020-01-01T00:00:00Z','EXPIRED-SUMMARY','{}','EXPIRED-OUTPUT','',0,0,'old-item',''),('busy-activity','conv','busy','command','running',1,'branch','2020-01-01T00:00:00Z','PROTECTED-SUMMARY','{}','PROTECTED-OUTPUT','',0,0,'busy-item','');
CREATE TABLE conversation_provider_events(id INTEGER PRIMARY KEY,conversation_id TEXT,session_id TEXT,payload_json TEXT,received_at TEXT,provider_event_id TEXT,method TEXT);
INSERT INTO conversation_provider_events VALUES(1,'conv','owner','{"providerTurnId":"native-old","text":"EXPIRED-RAW","error":null}','2020-01-01T00:00:00Z','old-raw','message.completed'),(2,'conv','owner','{"providerTurnId":"native-busy","text":"PROTECTED-RAW","error":null}','2020-01-01T00:00:00Z','busy-raw','message.completed');
CREATE TABLE agent_switches(session_id TEXT,state TEXT);CREATE TABLE review_run(session_id TEXT,status TEXT);
CREATE TABLE change_log(seq INTEGER PRIMARY KEY,created_at TEXT);INSERT INTO change_log VALUES(1,'2020-01-01T00:00:00Z');
CREATE TABLE telemetry_event(id TEXT PRIMARY KEY,occurred_at TEXT,payload_json TEXT);INSERT INTO telemetry_event VALUES('telemetry','2020-01-01T00:00:00Z','EXPIRED-TELEMETRY');
CREATE TABLE notifications(id TEXT PRIMARY KEY,dismissed_at TEXT,resolved_at TEXT,type TEXT,session_id TEXT);INSERT INTO notifications VALUES('notification','2020-01-01T00:00:00Z','2020-01-01T00:00:00Z','info','owner');
CREATE TABLE agent_switch_failure_outbox(id TEXT PRIMARY KEY,expires_at TEXT,payload TEXT);INSERT INTO agent_switch_failure_outbox VALUES('expired-outbox','2020-01-01T00:00:00Z','old'),('historically-not-expired','2026-10-26T00:00:00Z','retain');
CREATE TABLE agent_switch_failure_receipts(id TEXT PRIMARY KEY,retain_until TEXT);INSERT INTO agent_switch_failure_receipts VALUES('expired-receipt','2020-01-01T00:00:00Z'),('historically-not-expired','2026-10-26T00:00:00Z');`)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.EnsureHistoryRetentionSchema(db); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return path, hex.EncodeToString(h[:])
}
func testOptions(source, sha, dir string) Options {
	m := store.DefaultHistoryMaintenanceOptions()
	m.BatchRows = 2
	m.WriteBudget = time.Second
	return Options{Copy: rawcopy.Options{Source: source, Directory: dir, ExpectedSHA256: sha, MaxBytes: 1 << 20, Rows: 2, MaxBatches: 100, MaxRowBytes: 1 << 16, BatchBytes: 1 << 17, Budget: 30 * time.Second, BatchBudget: time.Second}, AsOf: testAsOf, Maintenance: m, MaxBatches: 100}
}
func TestHistoryCopyFullApplicationPassAndProtection(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "preview", true: "clean"}[clean], func(t *testing.T) {
			source, sha := fixture(t)
			before, _ := os.ReadFile(source)
			dir := filepath.Join(t.TempDir(), "fresh")
			o := testOptions(source, sha, dir)
			o.Clean = clean
			progress := []string{}
			o.Progress = func(s string, n int64) { progress = append(progress, s) }
			r, err := Run(context.Background(), o)
			if err != nil {
				t.Fatal(err, r)
			}
			after, _ := os.ReadFile(source)
			if string(before) != string(after) || r.SourceSHA256 != sha {
				t.Fatal("source changed")
			}
			if !r.AllocationBefore.Available || !r.AllocationAfter.Available || len(r.AllocationBefore.Objects) == 0 {
				t.Fatal("physical allocation evidence unavailable", r.AllocationBefore, r.AllocationAfter)
			}
			if clean && (!r.Complete || len(r.Stages) != 11 || r.Status != "complete") {
				t.Fatal(r)
			}
			if !clean && (r.Complete || len(r.Stages) != 0 || r.Status != "preview") {
				t.Fatal(r)
			}
			db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "raw-audit-working.sqlite3")+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var old, busy, closed, active string
			db.QueryRow(`SELECT text FROM conversation_messages WHERE id='old-message'`).Scan(&old)
			db.QueryRow(`SELECT text FROM conversation_messages WHERE id='busy-message'`).Scan(&busy)
			db.QueryRow(`SELECT prompt FROM sessions WHERE id='retired'`).Scan(&closed)
			db.QueryRow(`SELECT prompt FROM sessions WHERE id='owner'`).Scan(&active)
			if busy != "PROTECTED-MESSAGE" || active != "PROTECTED-SESSION" {
				t.Fatal("protected content changed")
			}
			if clean && (old != "[AO history expired]" || closed != "[AO history expired]") {
				t.Fatal("eligible content not cleared", old, closed)
			}
			if !clean && (old != "EXPIRED-MESSAGE" || closed != "EXPIRED-SESSION") {
				t.Fatal("preview changed content")
			}
			for _, table := range []string{"agent_switch_failure_outbox", "agent_switch_failure_receipts"} {
				var n int
				db.QueryRow(`SELECT count(*) FROM ` + quoted(table) + ` WHERE id='historically-not-expired'`).Scan(&n)
				if n != 1 {
					t.Fatal("used current instead of historical expiry clock", table)
				}
			}
			for _, table := range []string{"conversation_provider_events", "conversation_messages", "conversation_activities", "conversation_turns", "sessions"} {
				var a, b TableIdentity
				for _, v := range r.IdentityBefore {
					if v.Table == table {
						a = v
					}
				}
				for _, v := range r.IdentityAfter {
					if v.Table == table {
						b = v
					}
				}
				if a.Rows != b.Rows || a.RowIDMappingSHA256 != b.RowIDMappingSHA256 {
					t.Fatal("business identity changed", table)
				}
			}
			if clean && len(progress) == 0 {
				t.Fatal("no safe phase progress")
			}
			if r.RebuildPerformed {
				t.Fatal("unexpected rebuild")
			}
			if _, err = Run(context.Background(), o); err == nil {
				t.Fatal("existing target reopened")
			}
		})
	}
}
func TestHistoryCopyPartialAndHashMismatch(t *testing.T) {
	for _, kind := range []string{"batch-budget", "sha-mismatch", "future-clock"} {
		t.Run(kind, func(t *testing.T) {
			source, sha := fixture(t)
			dir := filepath.Join(t.TempDir(), "fresh")
			o := testOptions(source, sha, dir)
			o.Clean = true
			switch kind {
			case "batch-budget":
				o.MaxBatches = 1
				o.Copy.MaxBatches = 1
			case "sha-mismatch":
				o.Copy.ExpectedSHA256 = strings.Repeat("a", 64)
			case "future-clock":
				o.AsOf = time.Now().Add(time.Hour)
			}
			r, err := Run(context.Background(), o)
			if kind == "batch-budget" {
				if err != nil || r.Complete || r.Status != "partial" {
					t.Fatal(r, err)
				}
			} else if err == nil {
				t.Fatal("invalid manifest accepted")
			}
			if kind == "sha-mismatch" {
				b, e := os.ReadFile(filepath.Join(dir, "raw-audit-working.sqlite3"))
				original, _ := os.ReadFile(source)
				if e != nil || string(b) != string(original) {
					t.Fatal("hash mismatch changed copy before authorization", e)
				}
			}
		})
	}
}
func TestHistoryCopyIdentityPreservesImplicitRowIDMapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mapping.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE turns(id TEXT PRIMARY KEY);INSERT INTO turns(rowid,id)VALUES(9,'one'),(21,'two');CREATE TABLE integer_alias(id INTEGER PRIMARY KEY);INSERT INTO integer_alias VALUES(9);CREATE TABLE descending(id INTEGER PRIMARY KEY DESC);INSERT INTO descending VALUES(9);CREATE TABLE composite(a TEXT,b TEXT,PRIMARY KEY(a,b)) WITHOUT ROWID;INSERT INTO composite VALUES('one','two');`)
	if err != nil {
		t.Fatal(err)
	}
	before, err := CensusIdentity(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range before {
		if r.Table == "turns" && (r.IntegerAlias || r.RowIDMappingSHA256 == "") {
			t.Fatal(r)
		}
		if r.Table == "integer_alias" && !r.IntegerAlias {
			t.Fatal(r)
		}
		if r.Table == "descending" && r.IntegerAlias {
			t.Fatal(r)
		}
		if r.Table == "composite" && (!r.WithoutRowID || r.RowIDMappingSHA256 != "") {
			t.Fatal(r)
		}
	}
	_, err = db.Exec(`UPDATE turns SET rowid=10 WHERE id='one'`)
	if err != nil {
		t.Fatal(err)
	}
	after, err := CensusIdentity(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(before, after) {
		t.Fatal("implicit identity remap invisible")
	}
}
func TestHistoryCopyExecutableSmoke(t *testing.T) {
	binary := os.Getenv("AO_HISTORY_COPY_SMOKE_BINARY")
	if binary == "" {
		t.Skip("release smoke supplies an independently built executable")
	}
	source, sha := fixture(t)
	dir := filepath.Join(t.TempDir(), "fresh")
	cmd := exec.Command(binary, "--snapshot", source, "--expected-source-sha256", sha, "--new-working-directory", dir, "--as-of", testAsOf.Format(time.RFC3339Nano), "--clean-copy", "--wall-budget", "30s", "--read-budget", "2s", "--write-budget", "1s", "--max-batches", "100")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	b, err := cmd.Output()
	if err != nil {
		t.Fatal(err, stderr.String())
	}
	var r Report
	if err = json.Unmarshal(b, &r); err != nil || !r.Complete || r.RebuildPerformed {
		t.Fatal(r, err)
	}
	if !strings.Contains(stderr.String(), "history_copy_progress") {
		t.Fatal("no phase progress")
	}
}

func TestHistoryCopyPreservesApplicationForeignKeyEnforcement(t *testing.T) {
	source, _ := fixture(t)
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE telemetry_reference(id TEXT PRIMARY KEY,event TEXT REFERENCES telemetry_event(id) ON DELETE RESTRICT);INSERT INTO telemetry_reference VALUES('protected-reference','telemetry');`)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	o := testOptions(source, hex.EncodeToString(h[:]), filepath.Join(t.TempDir(), "fresh"))
	o.Clean = true
	r, err := Run(context.Background(), o)
	if err == nil || r.Complete || r.Status != "failed" {
		t.Fatal("foreign key restriction silently bypassed", r, err)
	}
	found := false
	for _, stage := range r.Stages {
		if stage.Stage == "builtin:telemetry_event" && stage.SQLiteCode != 0 && !stage.Complete {
			found = true
		}
	}
	if !found {
		t.Fatal("constraint failure not reported", r)
	}
}

func TestHistoryCopyCancellationNeverChangesSealedSource(t *testing.T) {
	source, sha := fixture(t)
	before, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	o := testOptions(source, sha, filepath.Join(t.TempDir(), "fresh"))
	o.Clean = true
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := Run(ctx, o)
	if err == nil || r.Complete {
		t.Fatal("canceled run completed", r, err)
	}
	after, err := os.ReadFile(source)
	if err != nil || string(before) != string(after) {
		t.Fatal("source changed on cancellation", err)
	}
}

func TestHistoryCopyAllocationAggregateMatchesPageCensus(t *testing.T) {
	source, _ := fixture(t)
	db, err := sql.Open("sqlite", "file:"+source+"?mode=ro&immutable=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := CensusAllocation(context.Background(), db)
	if err != nil || !r.Available {
		t.Fatal(r, err)
	}
	for _, o := range r.Objects {
		var pages, bytes, payload, unused int64
		if err = db.QueryRow(`SELECT COUNT(*),SUM(pgsize),SUM(payload),SUM(unused) FROM dbstat WHERE name=?`, o.Name).Scan(&pages, &bytes, &payload, &unused); err != nil {
			t.Fatal(err)
		}
		if pages != o.Pages || bytes != o.AllocatedBytes || payload != o.PayloadBytes || unused != o.UnusedBytes {
			t.Fatal("aggregate report changed physical units", o)
		}
	}
}

func TestHistoryCopyRefusesUnsupportedScanPrefixes(t *testing.T) {
	for _, kind := range []string{"zero-raw", "negative-raw", "empty-message", "null-message"} {
		t.Run(kind, func(t *testing.T) {
			source, _ := fixture(t)
			db, err := sql.Open("sqlite", source)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "zero-raw":
				_, err = db.Exec(`UPDATE conversation_provider_events SET id=0 WHERE id=1`)
			case "negative-raw":
				_, err = db.Exec(`UPDATE conversation_provider_events SET id=-1 WHERE id=1`)
			case "empty-message":
				_, err = db.Exec(`UPDATE conversation_messages SET id='' WHERE id='old-message'`)
			case "null-message":
				_, err = db.Exec(`UPDATE conversation_messages SET id=NULL WHERE id='old-message'`)
			}
			if err != nil {
				t.Fatal(err)
			}
			db.Close()
			b, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			h := sha256.Sum256(b)
			o := testOptions(source, hex.EncodeToString(h[:]), filepath.Join(t.TempDir(), "fresh"))
			o.Clean = true
			r, err := Run(context.Background(), o)
			if err != nil || r.Complete || r.Status != "partial" || r.StopReason != "unsupported_prefix" {
				t.Fatal("uncovered rows were declared fully scanned", r, err)
			}
			found := false
			for _, stage := range r.Stages {
				if stage.Prefix.UnsupportedRows == 1 && !stage.Complete && !stage.CoverageVerified && stage.StopReason == "unsupported_prefix" {
					found = true
				}
			}
			if !found {
				t.Fatal("missing unsupported count", r)
			}
		})
	}
}
func TestHistoryCopyIdentityRejectsInvalidTextKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-key.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE keyed(id TEXT PRIMARY KEY);INSERT INTO keyed VALUES(CAST(x'80' AS TEXT));`); err != nil {
		t.Fatal(err)
	}
	if _, err = CensusIdentity(context.Background(), db); err == nil {
		t.Fatal("invalid UTF8 identity silently normalized")
	}
}

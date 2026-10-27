//go:build linux

package rowidcopy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	aosqlite "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func snapshot(t *testing.T, ddl string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "sealed.sqlite3")
	db, e := sql.Open("sqlite", p)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(ddl); e != nil {
		db.Close()
		t.Fatal(e)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(p, 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func opts(t *testing.T, p string) Options {
	t.Helper()
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	h := sha256.Sum256(b)
	return Options{Source: p, Directory: filepath.Join(t.TempDir(), "new-candidate"), SourceSHA256: hex.EncodeToString(h[:]), Apply: true, MaxBytes: 16 << 20, Budget: 30 * time.Second}
}
func openCandidate(t *testing.T, o Options) *sql.DB {
	t.Helper()
	d, e := sql.Open("sqlite", filepath.Join(o.Directory, "candidate.sqlite3"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { d.Close() })
	return d
}
func TestRowIDRebuildTypedValuesTriggersSequencesAndCuts(t *testing.T) {
	p := snapshot(t, `
 PRAGMA user_version=23; PRAGMA application_id=165;
 CREATE TABLE audit(n INTEGER);
 CREATE TABLE conversation_turns(id TEXT PRIMARY KEY,requested_at TEXT,v ANY,b BLOB) STRICT;
 CREATE TRIGGER turns_insert AFTER INSERT ON conversation_turns BEGIN INSERT INTO audit VALUES(new.rowid); END;
 INSERT INTO conversation_turns(rowid,id,requested_at,v,b) VALUES(-7,'old','same',NULL,x'00ff'),(11,'middle','same',CAST(x'61ff0062' AS TEXT),x'0000'),(9007199254740993,'latest','same',1.25,x'feff');
 CREATE INDEX turn_time ON conversation_turns(requested_at);
 CREATE TABLE session_interface_transitions(id TEXT PRIMARY KEY,created_at TEXT);
 INSERT INTO session_interface_transitions(rowid,id,created_at) VALUES(2,'second','same'),(41,'last','same');
 CREATE TABLE shadow(rowid TEXT,_rowid_ TEXT,v BLOB);
 INSERT INTO shadow(oid,rowid,_rowid_,v) VALUES(901,'ordinary','also-ordinary',x'ff');
 CREATE TABLE integer_desc(id INTEGER PRIMARY KEY DESC,v TEXT);
 INSERT INTO integer_desc(rowid,id,v) VALUES(51,222,'independent-hidden-rowid');
 CREATE TABLE parent(id INTEGER PRIMARY KEY AUTOINCREMENT,v TEXT);
 INSERT INTO parent(id,v) VALUES(5,'kept'),(23,'deleted'); DELETE FROM parent WHERE id=23;
 UPDATE sqlite_sequence SET rowid=43,seq=1000 WHERE name='parent';
 CREATE TABLE child(id TEXT PRIMARY KEY,parent_id INTEGER REFERENCES parent(id));
 INSERT INTO child(rowid,id,parent_id) VALUES(71,'child',5);
 CREATE TABLE generated(id TEXT PRIMARY KEY,v INTEGER,g INTEGER GENERATED ALWAYS AS (v*2) STORED,h TEXT GENERATED ALWAYS AS ('v='||v) VIRTUAL);
 INSERT INTO generated(rowid,id,v) VALUES(97,'g',11);
 CREATE TABLE wr(a TEXT,b INTEGER,v ANY,PRIMARY KEY(a,b)) STRICT,WITHOUT ROWID;
 INSERT INTO wr VALUES('a',1,x'00ff'),('b',3,NULL);
 CREATE VIEW active_turns AS SELECT id,requested_at FROM conversation_turns;
 `)
	before, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	o := opts(t, p)
	r, e := Run(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	if r.Status != "complete_unpublished" || r.AutoVacuum != 2 || !r.SourceReadOnlyFDs || r.CandidateSHA256 == "" {
		t.Fatalf("missing proof: %+v", r)
	}
	after, e := os.ReadFile(p)
	if e != nil || string(before) != string(after) {
		t.Fatal("source changed")
	}
	d := openCandidate(t, o)
	var value int64
	for q, want := range map[string]int64{
		`SELECT count(*) FROM audit`: 3,
		`SELECT count(*) FROM conversation_turns WHERE rowid>= (SELECT rowid FROM conversation_turns WHERE id='middle')`: 2,
		`SELECT rowid FROM session_interface_transitions ORDER BY created_at DESC,rowid DESC LIMIT 1`:                    41,
		`SELECT rowid FROM integer_desc WHERE id=222`:                                                                    51,
		`SELECT oid FROM shadow`:                                901,
		`SELECT rowid FROM sqlite_sequence WHERE name='parent'`: 43,
		`SELECT seq FROM sqlite_sequence WHERE name='parent'`:   1000,
		`PRAGMA user_version`:                                   23, `PRAGMA application_id`: 165,
	} {
		if e = d.QueryRow(q).Scan(&value); e != nil || value != want {
			t.Fatalf("query failed %v got %d want %d", e, value, want)
		}
	}
	var typ, hexv string
	if e = d.QueryRow(`SELECT typeof(v),hex(v) FROM conversation_turns WHERE id='middle'`).Scan(&typ, &hexv); e != nil || typ != "text" || hexv != "61FF0062" {
		t.Fatal("NUL/invalid UTF8 text changed", typ, hexv, e)
	}
	if _, e = d.Exec(`INSERT INTO parent(v) VALUES('next')`); e != nil {
		t.Fatal(e)
	}
	if e = d.QueryRow(`SELECT max(id) FROM parent`).Scan(&value); e != nil || value != 1001 {
		t.Fatal("autoincrement high watermark lost", value, e)
	}
	if _, e = Run(context.Background(), o); e == nil {
		t.Fatal("existing candidate directory accepted")
	}
}
func TestRowIDRebuildCurrentApplicationSchema(t *testing.T) {
	dir := t.TempDir()
	s, e := aosqlite.Open(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(dir, "sealed.sqlite3")
	if e = os.Rename(filepath.Join(dir, "ao.db"), p); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(p, 0600); e != nil {
		t.Fatal(e)
	}
	o := opts(t, p)
	r, e := Run(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	if r.SchemaObjects < 100 || len(r.Tables) < 40 || r.AutoVacuum != 2 {
		t.Fatal("application schema not rebuilt", r.SchemaObjects, len(r.Tables))
	}
}
func TestRowIDRebuildRejectsUnprovableSchemas(t *testing.T) {
	for name, ddl := range map[string]string{
		"all-rowid-names-shadowed": `CREATE TABLE t(rowid TEXT,_rowid_ TEXT,oid TEXT);`,
		"virtual-or-shadow":        `CREATE VIRTUAL TABLE t USING fts5(body);`,
		"internal-stat-table":      `CREATE TABLE t(x);INSERT INTO t VALUES(1);ANALYZE;`,
		"foreign-key-violation":    `CREATE TABLE p(id INTEGER PRIMARY KEY);CREATE TABLE c(id INTEGER PRIMARY KEY,pid REFERENCES p(id));INSERT INTO c VALUES(4,99);`,
	} {
		t.Run(name, func(t *testing.T) {
			p := snapshot(t, ddl)
			o := opts(t, p)
			if _, e := Run(context.Background(), o); e == nil {
				t.Fatal("unsupported schema or broken FK accepted")
			}
		})
	}
}
func TestRowIDRebuildRefusesLivePathsAliasesAndUnknownInputs(t *testing.T) {
	for _, kind := range []string{"ao.db", "sidecar", "symlink", "hardlink", "mode", "hash", "budget", "cancelled", "existing-directory", "relative", "source-reference"} {
		t.Run(kind, func(t *testing.T) {
			p := snapshot(t, `CREATE TABLE t(id TEXT PRIMARY KEY);INSERT INTO t(rowid,id) VALUES(89,'id');`)
			o := opts(t, p)
			ctx := context.Background()
			switch kind {
			case "ao.db":
				q := filepath.Join(filepath.Dir(p), "ao.db")
				if e := os.Rename(p, q); e != nil {
					t.Fatal(e)
				}
				o.Source = q
			case "sidecar":
				if e := os.WriteFile(p+"-wal", nil, 0600); e != nil {
					t.Fatal(e)
				}
			case "symlink":
				q := p + ".link"
				if e := os.Symlink(p, q); e != nil {
					t.Fatal(e)
				}
				o.Source = q
			case "hardlink":
				if e := os.Link(p, p+".hardlink"); e != nil {
					t.Fatal(e)
				}
			case "mode":
				if e := os.Chmod(p, 0644); e != nil {
					t.Fatal(e)
				}
			case "hash":
				o.SourceSHA256 = strings.Repeat("a", 64)
			case "budget":
				o.MaxBytes = 1
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "existing-directory":
				if e := os.Mkdir(o.Directory, 0700); e != nil {
					t.Fatal(e)
				}
			case "relative":
				o.Source = "sealed.sqlite3"
			case "source-reference":
				if path := os.Getenv("AO_ROWID_REFERENCE_FIXTURE"); path != "" {
					info, e := os.Stat(path)
					if e != nil {
						t.Fatal(e)
					}
					if e = noOtherReferences(ctx, info); e == nil {
						t.Fatal("foreign FD ignored")
					}
					return
				}
				f, e := os.Open(p)
				if e != nil {
					t.Fatal(e)
				}
				defer f.Close()
				binary, e := os.Executable()
				if e != nil {
					t.Fatal(e)
				}
				c := exec.Command(binary, "-test.run=^TestRowIDRebuildRefusesLivePathsAliasesAndUnknownInputs/source-reference$")
				c.Env = append(os.Environ(), "AO_ROWID_REFERENCE_FIXTURE="+p)
				if out, e := c.CombinedOutput(); e != nil {
					t.Fatalf("reference fixture failed: %v %s", e, out)
				}
				return
			}
			if _, e := Run(ctx, o); e == nil {
				t.Fatal("unsafe input accepted")
			}
		})
	}
}
func TestRowIDRebuildPreviewAndReadOnlyFDProof(t *testing.T) {
	p := snapshot(t, `CREATE TABLE t(id TEXT PRIMARY KEY);INSERT INTO t(rowid,id) VALUES(12,'id');`)
	o := opts(t, p)
	o.Apply = false
	r, e := Run(context.Background(), o)
	if e != nil {
		t.Fatal(e)
	}
	if r.Status != "preview" || !r.SourceReadOnlyFDs || r.AutoVacuum != 0 || r.CandidateSHA256 != "" {
		t.Fatal(r)
	}
	info, e := os.Stat(p)
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(p, os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if sourceFDsReadOnly(info) {
		t.Fatal("read-write source FD accepted as read-only proof")
	}
}
func TestRowIDRebuildUTF16EncodingAndQuotedNames(t *testing.T) {
	p := snapshot(t, `PRAGMA encoding='UTF-16le';CREATE TABLE "quoted""table"("id""key" TEXT PRIMARY KEY,"field with space" TEXT);INSERT INTO "quoted""table"(rowid,"id""key","field with space") VALUES(333,'k','text');`)
	o := opts(t, p)
	if _, e := Run(context.Background(), o); e != nil {
		t.Fatal(e)
	}
	d := openCandidate(t, o)
	var encoding string
	if e := d.QueryRow(`PRAGMA encoding`).Scan(&encoding); e != nil || encoding != "UTF-16le" {
		t.Fatal(encoding, e)
	}
}

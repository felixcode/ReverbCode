//go:build linux

package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRowIDCopyCLIParsingAndFailure(t *testing.T) {
	for _, test := range []struct {
		args []string
		code int
	}{
		{[]string{"-h"}, 0}, {[]string{"--unknown"}, 2}, {[]string{"--wall-budget", "invalid"}, 2}, {[]string{"positional"}, 2}, {nil, 1},
	} {
		var out, err bytes.Buffer
		if got := execute(test.args, &out, &err); got != test.code {
			t.Fatalf("exit %d want %d", got, test.code)
		}
		if out.Len() != 0 {
			t.Fatal("failure/help emitted success report")
		}
	}
}
func TestRowIDCopyCLIFreshCandidateSmoke(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "sealed.sqlite3")
	db, err := sql.Open("sqlite", source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE t(id TEXT PRIMARY KEY,body TEXT);INSERT INTO t(rowid,id,body) VALUES(123456789012345678,'fixture','private fixture');`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(source, 0600); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(b)
	var out, stderr bytes.Buffer
	args := []string{"--snapshot", source, "--source-sha256", hex.EncodeToString(sum[:]), "--new-candidate-directory", filepath.Join(dir, "candidate"), "--rebuild", "--max-database-bytes", "1048576", "--wall-budget", "30s"}
	if code := execute(args, &out, &stderr); code != 0 {
		t.Fatal("smoke failed", code, stderr.String())
	}
	var r map[string]any
	if err = json.Unmarshal(out.Bytes(), &r); err != nil {
		t.Fatal(err)
	}
	if r["status"] != "complete_unpublished" || r["auto_vacuum"] != float64(2) || r["source_read_only_fds"] != true || r["source_integrity_checked"] != true {
		t.Fatal("missing smoke proof")
	}
	if bytes.Contains(out.Bytes(), []byte("private fixture")) {
		t.Fatal("body leaked")
	}
}

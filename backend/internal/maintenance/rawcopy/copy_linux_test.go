//go:build linux

package rawcopy

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sealedFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sealed.sqlite3")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE ao_history_retention(object_kind TEXT NOT NULL,object_id TEXT NOT NULL,conversation_id TEXT NOT NULL,semantic_sha256 TEXT NOT NULL,prompt_sha256 TEXT NOT NULL,original_bytes INTEGER NOT NULL,original_sha256 TEXT NOT NULL,expired_at TEXT NOT NULL,cutoff_at TEXT NOT NULL,fields_json TEXT NOT NULL,PRIMARY KEY(object_kind,object_id));CREATE TABLE conversation_provider_events(id INTEGER PRIMARY KEY,conversation_id TEXT,session_id TEXT,provider_event_id TEXT,method TEXT,payload_json TEXT,received_at TEXT);`)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}
func options(source, dir string) Options {
	return Options{Source: source, Directory: dir, MaxBytes: 1 << 20, Rows: 10, MaxBatches: 2, MaxRowBytes: 1 << 16, BatchBytes: 1 << 17, Budget: 5 * time.Second, BatchBudget: time.Second}
}
func TestRawCopyDefaultPreviewAndApplyOnlyFreshCopy(t *testing.T) {
	for _, apply := range []bool{false, true} {
		t.Run(map[bool]string{false: "preview", true: "apply"}[apply], func(t *testing.T) {
			source := sealedFixture(t)
			before, err := os.ReadFile(source)
			if err != nil {
				t.Fatal(err)
			}
			dir := filepath.Join(t.TempDir(), "new-copy")
			o := options(source, dir)
			o.Apply = apply
			report, err := Run(context.Background(), o)
			if err != nil {
				t.Fatal(err)
			}
			if !report.Complete || report.Migrated != 0 || report.SourceSHA256 == "" {
				t.Fatal(report)
			}
			after, _ := os.ReadFile(source)
			if string(before) != string(after) {
				t.Fatal("source changed")
			}
			s, _ := os.Stat(source)
			c, _ := os.Stat(filepath.Join(dir, "raw-audit-working.sqlite3"))
			if os.SameFile(s, c) || c.Mode().Perm() != 0600 {
				t.Fatal("not private independent copy")
			}
			if _, err = Run(context.Background(), o); err == nil {
				t.Fatal("existing directory apply allowed")
			}
		})
	}
}
func TestRawCopyRefusesLiveAliasAndUnknownInput(t *testing.T) {
	for _, name := range []string{"live-name", "sidecar", "mode", "symlink", "budget", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			source := sealedFixture(t)
			o := options(source, filepath.Join(t.TempDir(), "fresh"))
			ctx := context.Background()
			switch name {
			case "live-name":
				path := filepath.Join(filepath.Dir(source), "ao.db")
				if err := os.Rename(source, path); err != nil {
					t.Fatal(err)
				}
				o.Source = path
			case "sidecar":
				os.WriteFile(source+"-wal", nil, 0600)
			case "mode":
				os.Chmod(source, 0644)
			case "symlink":
				p := source + ".link"
				os.Symlink(source, p)
				o.Source = p
			case "budget":
				o.MaxBytes = 1
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := Run(ctx, o); err == nil {
				t.Fatal("unsafe input accepted")
			}
		})
	}
}
func TestRawCopyReferenceGuardDetectsOtherFD(t *testing.T) {
	if path := os.Getenv("AO_SYNTHETIC_RAW_COPY_FD"); path != "" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		err = noOtherReferences(context.Background(), info)
		if err == nil || !strings.Contains(err.Error(), "reference") {
			t.Fatal("other process FD not refused", err)
		}
		return
	}
	source := sealedFixture(t)
	fd, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(binary, "-test.run=^TestRawCopyReferenceGuardDetectsOtherFD$")
	command.Env = append(os.Environ(), "AO_SYNTHETIC_RAW_COPY_FD="+source)
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("reference probe failed %v %s", err, out)
	}
}

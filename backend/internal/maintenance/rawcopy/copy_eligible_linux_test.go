//go:build linux

package rawcopy

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
	"testing"
)

func eligibleSealedFixture(t *testing.T) (string, string, string) {
	t.Helper()
	path := sealedFixture(t)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	original := `{"error":null,"providerTurnId":"native-old","text":"expired-synthetic"}`
	rawHash := sha256.Sum256([]byte(original))
	canonical, err := json.Marshal(map[string]string{"payload_json": original})
	if err != nil {
		t.Fatal(err)
	}
	mapHash := sha256.Sum256(canonical)
	rawSHA, mapSHA := hex.EncodeToString(rawHash[:]), hex.EncodeToString(mapHash[:])
	if rawSHA == mapSHA {
		t.Fatal("fixture digest domains collapsed")
	}
	inline := map[string]any{"error": nil, "providerTurnId": "native-old", "text": "", "_retention": map[string]any{
		"protocol": "ao-history-v1", "expired_at": "2026-10-27T10:00:00Z", "cutoff_at": "2026-10-25T11:00:00Z", "fields": []string{"text"}, "original_bytes": len(original), "original_sha256": rawSHA,
	}}
	encoded, err := json.Marshal(inline)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO conversation_provider_events VALUES(1,'conv','owner','native-event','message.completed',?,'2020-01-01T00:00:00Z');`, string(encoded))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO ao_history_retention VALUES('provider_event','1','conv','','',?,?,'2026-10-27T10:00:00Z','2026-10-25T11:00:00Z','["text"]');`, len(original), mapSHA)
	if err != nil {
		t.Fatal(err)
	}
	// A sidecar without full inline proof and an unrelated marker must survive.
	_, err = db.Exec(`INSERT INTO conversation_provider_events VALUES(2,'conv','owner','other-event','message.completed','{"error":null}','2020-01-01T00:00:00Z');
	INSERT INTO ao_history_retention VALUES('provider_event','2','conv','','',42,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','2026-10-27T10:00:00Z','2026-10-25T11:00:00Z','["text"]');
	INSERT INTO ao_history_retention VALUES('message','message','conv','','',42,'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','2026-10-27T10:00:00Z','2026-10-25T11:00:00Z','["text"]');`)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return path, rawSHA, mapSHA
}

func TestRawCopyEligibleEndToEnd(t *testing.T) {
	for _, executable := range []bool{false, true} {
		binary := os.Getenv("AO_RAW_AUDIT_COPY_SMOKE_BINARY")
		if executable && binary == "" {
			continue // The targeted release smoke supplies its independently built CLI.
		}
		for _, apply := range []bool{false, true} {
			name := map[bool]string{false: "library", true: "executable"}[executable] + "/" + map[bool]string{false: "preview", true: "apply"}[apply]
			t.Run(name, func(t *testing.T) {
				source, rawSHA, mapSHA := eligibleSealedFixture(t)
				before, err := os.ReadFile(source)
				if err != nil {
					t.Fatal(err)
				}
				dir := filepath.Join(t.TempDir(), "fresh")
				o := options(source, dir)
				o.Apply = apply
				var report Report
				if executable {
					args := []string{"--snapshot", source, "--new-working-directory", dir, "--wall-budget", "5s", "--batch-budget", "1s"}
					if apply {
						args = append(args, "--apply")
					}
					output, e := exec.Command(binary, args...).CombinedOutput()
					if e != nil {
						t.Fatalf("copy CLI failed: %v %s", e, output)
					}
					if err = json.Unmarshal(output, &report); err != nil {
						t.Fatal(err)
					}
				} else {
					report, err = Run(context.Background(), o)
					if err != nil {
						t.Fatal(err)
					}
				}
				wantMigrated := int64(0)
				if apply {
					wantMigrated = 1
				}
				if !report.Complete || report.Eligible != 1 || report.Migrated != wantMigrated || report.Retained != 1 {
					t.Fatal(report)
				}
				after, err := os.ReadFile(source)
				if err != nil || string(before) != string(after) {
					t.Fatal("sealed source changed", err)
				}
				copyPath := filepath.Join(dir, "raw-audit-working.sqlite3")
				db, err := sql.Open("sqlite", "file:"+copyPath+"?mode=ro")
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				var payload, conv, owner, event, method, received string
				if err = db.QueryRow(`SELECT payload_json,conversation_id,session_id,provider_event_id,method,received_at FROM conversation_provider_events WHERE id=1`).Scan(&payload, &conv, &owner, &event, &method, &received); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual([]string{conv, owner, event, method, received}, []string{"conv", "owner", "native-event", "message.completed", "2020-01-01T00:00:00Z"}) {
					t.Fatal("identity/header metadata changed")
				}
				var obj map[string]any
				if err = json.Unmarshal([]byte(payload), &obj); err != nil {
					t.Fatal(err)
				}
				if obj["text"] != "" || obj["error"] != nil || obj["providerTurnId"] != "native-old" {
					t.Fatal("body revived or raw headers changed")
				}
				audit := obj["_retention"].(map[string]any)
				if audit["original_sha256"] != rawSHA {
					t.Fatal("raw digest lost")
				}
				binding, exists := audit["field_map_audit"]
				if exists != apply || (apply && !reflect.DeepEqual(binding, map[string]any{"id": "1", "conversation": "conv", "sha256": mapSHA})) {
					t.Fatal("second digest/binding not preserved")
				}
				var total, message, orphan int
				db.QueryRow(`SELECT COUNT(*) FROM ao_history_retention`).Scan(&total)
				db.QueryRow(`SELECT COUNT(*) FROM ao_history_retention WHERE object_kind='message'`).Scan(&message)
				db.QueryRow(`SELECT COUNT(*) FROM ao_history_retention WHERE object_kind='provider_event' AND object_id='2'`).Scan(&orphan)
				if total != 3-int(wantMigrated) || message != 1 || orphan != 1 {
					t.Fatal("unproven or unrelated marker removed", total, message, orphan)
				}
				info, err := os.Stat(filepath.Join(dir, "raw-audit-copy-report.json"))
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatal("private report missing", err)
				}
			})
		}
	}
}

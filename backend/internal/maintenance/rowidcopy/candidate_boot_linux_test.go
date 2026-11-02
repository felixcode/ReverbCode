//go:build linux

package rowidcopy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"syscall"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	aosqlite "github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

var candidateMountPath = regexp.MustCompile(`^/snapshot/ao-copy-[0-9a-f]{32}/candidate\.sqlite3$`)
var candidateDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

func candidateToken(info os.FileInfo) []uint64 {
	s := info.Sys().(*syscall.Stat_t)
	return []uint64{s.Dev, s.Ino, uint64(s.Uid), uint64(s.Gid), uint64(info.Mode().Perm()), s.Nlink, uint64(info.Size())}
}

func validCandidateInput(path, sha string, token []uint64) bool {
	return candidateMountPath.MatchString(path) && candidateDigest.MatchString(sha) && len(token) == 7 &&
		token[2] == 1000 && token[3] == 1000 && token[4] == 0600 && token[5] == 1 && token[6] > 0
}

// The caller must also prove the closed source volume's label/nonce and absence
// of ALL container references. PID isolation or this readonly FD alone is not
// that proof. Only the fresh target receives writes or an AO file name.
func copyClosedCandidate(source, sha, workRoot string, expected []uint64) (string, error) {
	for p := source; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("candidate_symlink_or_missing")
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(source + suffix); !os.IsNotExist(err) {
			return "", errors.New("candidate_sidecar")
		}
	}
	input, err := os.OpenFile(source, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", errors.New("candidate_open")
	}
	defer input.Close()
	before, err := input.Stat()
	if err != nil || !before.Mode().IsRegular() || !reflect.DeepEqual(candidateToken(before), expected) {
		return "", errors.New("candidate_token_mismatch")
	}
	root, err := os.Lstat(workRoot)
	if err != nil || !root.IsDir() || root.Mode()&os.ModeSymlink != 0 || root.Mode().Perm() != 0700 {
		return "", errors.New("work_root_not_private")
	}
	dir, err := os.MkdirTemp(workRoot, "ao-private-boot-")
	if err != nil {
		return "", errors.New("new_work_directory")
	}
	output, err := os.OpenFile(filepath.Join(dir, "ao.db"), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", errors.New("new_database_exclusive_create")
	}
	defer output.Close()
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(output, hash), io.LimitReader(input, before.Size()+1))
	if err != nil || n != before.Size() || hex.EncodeToString(hash.Sum(nil)) != sha {
		return "", errors.New("candidate_copy_digest_or_length")
	}
	after, err := input.Stat()
	pathInfo, pathErr := os.Lstat(source)
	if err != nil || pathErr != nil || !os.SameFile(before, pathInfo) || (!reflect.DeepEqual(candidateToken(before), candidateToken(after)) || before.Sys().(*syscall.Stat_t).Mtim != after.Sys().(*syscall.Stat_t).Mtim || before.Sys().(*syscall.Stat_t).Ctim != after.Sys().(*syscall.Stat_t).Ctim) {
		return "", errors.New("candidate_changed_during_copy")
	}
	created, err := output.Stat()
	if err != nil || os.SameFile(before, created) || created.Mode().Perm() != 0600 || candidateToken(created)[5] != 1 {
		return "", errors.New("copy_identity_or_mode")
	}
	if err = output.Sync(); err != nil {
		return "", errors.New("copy_sync")
	}
	if err = output.Close(); err != nil {
		return "", errors.New("copy_close")
	}
	directory, err := os.Open(dir)
	if err != nil {
		return "", errors.New("copy_directory_open")
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return "", errors.New("copy_directory_sync")
	}
	return dir, nil
}

func candidateMeta(t *testing.T, data string) int64 {
	t.Helper()
	u := url.URL{Scheme: "file", Path: filepath.Join(data, "ao.db")}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro")
	if err != nil {
		t.Fatal("private_meta_open")
	}
	defer db.Close()
	var auto, sessions int64
	if err = db.QueryRow(`PRAGMA auto_vacuum`).Scan(&auto); err != nil || auto != 2 {
		t.Fatal("candidate_auto_vacuum_unsupported")
	}
	if err = db.QueryRow(`SELECT count(*) FROM sessions`).Scan(&sessions); err != nil {
		t.Fatal("candidate_session_count")
	}
	return sessions
}

func privateBootFiles(t *testing.T, data string) map[string]any {
	t.Helper()
	state := map[string]any{}
	for _, name := range []string{"ao.db", "ao.db-wal", "ao.db-shm"} {
		info, err := os.Lstat(filepath.Join(data, name))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal("private_file_metadata")
		}
		state[name] = []any{candidateToken(info), info.ModTime().UnixNano()}
	}
	return state
}

func TestClosedCandidateBoot(t *testing.T) {
	source := os.Getenv("AO_RECOVERY_CANDIDATE_SOURCE")
	if source == "" {
		t.Skip("closed_candidate_wrapper_only")
	}
	sha := os.Getenv("AO_RECOVERY_CANDIDATE_SHA256")
	var token []uint64
	if json.Unmarshal([]byte(os.Getenv("AO_RECOVERY_CANDIDATE_TOKEN")), &token) != nil || !validCandidateInput(source, sha, token) {
		t.Fatal("closed_candidate_input_rejected")
	}
	deadline := time.Now().Add(120 * time.Second)
	for {
		if f, err := os.Lstat("/work/boot-approved"); err == nil && f.Mode().IsRegular() && f.Mode().Perm() == 0600 && candidateToken(f)[5] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("isolation_gate_timeout")
		}
		time.Sleep(50 * time.Millisecond)
	}
	data, err := copyClosedCandidate(source, sha, "/work", token)
	if err != nil {
		t.Fatal(err)
	}
	initialFiles := privateBootFiles(t, data)
	count := candidateMeta(t, data)
	// This Store open verifies the real current schema, before the same daemon
	// helper used by the accepted empty-fixture smoke opens the private copy.
	s, err := aosqlite.OpenPreMigrated(data)
	if err != nil {
		t.Fatal("candidate_application_store_open")
	}
	s.Close()
	bootRecoveryData(t, "/tool/ao", data, false)
	finalFiles := privateBootFiles(t, data)
	value := map[string]any{"status": "private_candidate_boot_passed", "source_sha256": sha,
		"copy_bytes": token[6], "session_rows_before": count, "source_writes": false,
		"private_copy_new_writes_possible": true, "application_startup_writes_observed": !reflect.DeepEqual(initialFiles, finalFiles),
		"private_files_before": initialFiles, "private_files_after": finalFiles, "health_ready_own_pid": true, "graceful_stop": true,
		"actual_native_replay": "U_not_exercised", "provider_compatibility": "U_not_exercised"}
	bytes, _ := json.Marshal(value)
	if err = os.WriteFile("/work/boot-result-safe.json", append(bytes, '\n'), 0600); err != nil {
		t.Fatal("safe_result_write")
	}
}

func TestClosedCandidateInputAndCopyBoundaries(t *testing.T) {
	for _, kind := range []string{"valid", "wrong_digest", "wrong_token", "sidecar", "symlink"} {
		t.Run(kind, func(t *testing.T) {
			source := snapshot(t, `CREATE TABLE fixture(id INTEGER PRIMARY KEY); INSERT INTO fixture VALUES(1);`)
			b, _ := os.ReadFile(source)
			sum := sha256.Sum256(b)
			sha := hex.EncodeToString(sum[:])
			info, _ := os.Stat(source)
			token := candidateToken(info)
			switch kind {
			case "wrong_digest":
				sha = "0000000000000000000000000000000000000000000000000000000000000000"
			case "wrong_token":
				token[1]++
			case "sidecar":
				os.WriteFile(source+"-wal", nil, 0600)
			case "symlink":
				link := filepath.Join(t.TempDir(), "alias.sqlite3")
				os.Symlink(source, link)
				source = link
			}
			work := t.TempDir()
			os.Chmod(work, 0700)
			_, err := copyClosedCandidate(source, sha, work, token)
			want := map[string]string{"wrong_digest": "candidate_copy_digest_or_length", "wrong_token": "candidate_token_mismatch", "sidecar": "candidate_sidecar", "symlink": "candidate_symlink_or_missing"}
			if (err == nil) != (kind == "valid") || err != nil && err.Error() != want[kind] {
				t.Fatal("copy_boundary_mismatch", kind, err)
			}
		})
	}
	valid := "/snapshot/ao-copy-00000000000000000000000000000000/candidate.sqlite3"
	sha := "0000000000000000000000000000000000000000000000000000000000000000"
	token := []uint64{1, 2, 1000, 1000, 0600, 1, 4096}
	for _, path := range []string{"/home/coland/.ao/ao.db", "/snapshot/ao-copy-00000000000000000000000000000000/raw-audit-working.sqlite3", "/snapshot/../candidate.sqlite3", valid} {
		if validCandidateInput(path, sha, token) != (path == valid) {
			t.Fatal("fixed_candidate_path_boundary")
		}
	}
}

func TestClosedNonemptyCandidateCopiesAndBootsWithoutNativeProvider(t *testing.T) {
	binary := os.Getenv("AO_RECOVERY_SMOKE_BINARY")
	if binary == "" {
		t.Skip("built_binary_required")
	}
	data := rebuildRecoveryFixture(t, nonemptyBootSource(t))
	source := filepath.Join(data, "ao.db")
	contents, _ := os.ReadFile(source)
	sum := sha256.Sum256(contents)
	info, _ := os.Stat(source)
	work := t.TempDir()
	os.Chmod(work, 0700)
	copy, err := copyClosedCandidate(source, hex.EncodeToString(sum[:]), work, candidateToken(info))
	if err != nil || candidateMeta(t, copy) != 1 {
		t.Fatal("nonempty_private_copy", err)
	}
	bootRecoveryData(t, binary, copy, false)
	after, _ := os.ReadFile(source)
	if !reflect.DeepEqual(contents, after) {
		t.Fatal("original_candidate_changed")
	}
}

func nonemptyBootSource(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	s, err := aosqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err = s.UpsertProject(ctx, domain.ProjectRecord{ID: "closed", Path: "/unmounted/fixture-project", RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	rec, err := s.CreateSession(ctx, domain.SessionRecord{ID: "closed-1", ProjectID: "closed", Harness: domain.HarnessCodex, Mode: domain.SessionModeChat, Kind: domain.KindWorker, IsTerminated: true, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.CreateConversation(ctx, "closed-conv", domain.ConversationScopeSession, rec.ProjectID, rec.ID, now)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ClaimChatControllerGeneration(ctx, rec.ID, "fixture-generation"); err != nil {
		t.Fatal(err)
	}
	msg := domain.ConversationMessage{ID: "retained-original", ClientMessageID: "retained-client", Text: "synthetic", Origin: domain.MessageOriginHuman}
	if created, err := s.AppendUserMessage(ctx, c.ID, rec.ID, "fixture-generation", msg, "retained-turn", now); err != nil || !created {
		t.Fatal("nonempty_fixture_input", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	return sealRecoveryFixture(t, dir)
}

func TestExportNonemptyBootSource(t *testing.T) {
	if os.Getenv("AO_RECOVERY_FIXTURE_EXPORT") != "/out/sealed-nonempty.sqlite3" {
		t.Skip("fixture_builder_only")
	}
	input, err := os.Open(nonemptyBootSource(t))
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	output, err := os.OpenFile("/out/sealed-nonempty.sqlite3", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	if _, err = io.Copy(output, input); err != nil {
		t.Fatal(err)
	}
	if err = output.Sync(); err != nil {
		t.Fatal(err)
	}
}

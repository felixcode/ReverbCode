//go:build linux

// Package rawcopy never opens its source for writing. Apply is restricted to a
// fresh, exclusively-created working copy, not an arbitrary DB path.
package rawcopy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
	_ "modernc.org/sqlite"
)

type Options struct {
	Source, Directory       string
	Apply                   bool
	MaxBytes                int64
	Rows, MaxBatches        int
	MaxRowBytes, BatchBytes int64
	Budget, BatchBudget     time.Duration
}
type Report struct {
	Status                                  string `json:"status"`
	Apply                                   bool   `json:"apply"`
	SourceSHA256                            string `json:"source_sha256"`
	SourceBytes                             int64  `json:"source_bytes"`
	Scanned, Eligible, Migrated, Retained   int64
	LogicalBytesDelta                       int64 `json:"logical_bytes_delta"`
	PhysicalBytesBefore, PhysicalBytesAfter int64
	Complete                                bool `json:"complete"`
}

func privateRegular(info os.FileInfo) bool {
	st, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Mode().Perm() == 0600 && st.Uid == uint32(os.Geteuid()) && st.Nlink == 1
}
func stable(a, b os.FileInfo) bool {
	x, xok := a.Sys().(*syscall.Stat_t)
	y, yok := b.Sys().(*syscall.Stat_t)
	return xok && yok && x.Dev == y.Dev && x.Ino == y.Ino && x.Size == y.Size && x.Mtim == y.Mtim && x.Ctim == y.Ctim
}
func noSidecars(path string) error {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		_, err := os.Lstat(path + suffix)
		if !os.IsNotExist(err) {
			return errors.New("source_has_sidecar_or_unknown")
		}
	}
	return nil
}
func localPath(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("absolute_clean_path_required")
	}
	// Reject network/unknown mounts from local kernel metadata before any stat.
	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		halves := strings.SplitN(line, " - ", 2)
		if len(halves) != 2 {
			continue
		}
		left := strings.Fields(halves[0])
		right := strings.Fields(halves[1])
		if len(left) < 5 || len(right) == 0 {
			return errors.New("mount_metadata_unknown")
		}
		mount := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(left[4])
		if path == mount || strings.HasPrefix(path, strings.TrimSuffix(mount, "/")+"/") {
			switch right[0] {
			case "ext4", "ext3", "ext2", "xfs", "btrfs", "tmpfs", "overlay":
			default:
				return errors.New("nonlocal_or_unknown_filesystem")
			}
		}
	}
	current := "/"
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		info, e := os.Lstat(current)
		if e != nil {
			return e
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("symlink_path")
		}
	}
	return nil
}

// noOtherReferences examines local /proc metadata only. Any unreadable live
// process is unknown and refuses apply; no cmdline, environment or body is read.
func noOtherReferences(ctx context.Context, info os.FileInfo) error {
	own := info.Sys().(*syscall.Stat_t)
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err = ctx.Err(); err != nil {
			return err
		}
		pid, e := strconv.Atoi(entry.Name())
		if e != nil || pid == os.Getpid() {
			continue
		}
		root := filepath.Join("/proc", entry.Name())
		fds, e := os.ReadDir(filepath.Join(root, "fd"))
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return errors.New("process_references_unknown")
		}
		for _, fd := range fds {
			// fdinfo inode does not dereference a possibly hard-mounted NFS descriptor.
			text, e := os.ReadFile(filepath.Join(root, "fdinfo", fd.Name()))
			if os.IsNotExist(e) {
				continue
			}
			if e != nil {
				return errors.New("process_references_unknown")
			}
			for _, line := range strings.Split(string(text), "\n") {
				if strings.HasPrefix(line, "ino:") {
					n, e := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "ino:")), 10, 64)
					if e != nil {
						return errors.New("process_references_unknown")
					}
					if n == own.Ino {
						return errors.New("copy_has_possible_process_reference")
					}
				}
			}
		}
		maps, e := os.ReadFile(filepath.Join(root, "maps"))
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return errors.New("process_references_unknown")
		}
		for _, line := range strings.Split(string(maps), "\n") {
			parts := strings.Fields(line)
			if len(parts) < 5 {
				continue
			}
			inode, e := strconv.ParseUint(parts[4], 10, 64)
			if e == nil && inode == own.Ino {
				return errors.New("copy_has_possible_mapping_reference")
			}
		}
	}
	return nil
}

// Run creates Directory itself (must not exist), copies a sealed local snapshot
// into fixed raw-audit-working.sqlite3, and only ever writes that new inode.
// A failed operation leaves private evidence/copy for explicit owner retirement.
func Run(parent context.Context, o Options) (report Report, err error) {
	report.Apply = o.Apply
	if o.MaxBytes <= 0 || o.Rows <= 0 || o.MaxBatches <= 0 || o.MaxRowBytes <= 0 || o.BatchBytes < o.MaxRowBytes || o.Budget <= 0 || o.BatchBudget <= 0 {
		return report, errors.New("invalid_budget")
	}
	ctx, cancel := context.WithTimeout(parent, o.Budget)
	defer cancel()
	if filepath.Base(o.Source) == "ao.db" {
		return report, errors.New("live_database_name_rejected")
	}
	if err = localPath(o.Source); err != nil {
		return report, err
	}
	if err = localPath(filepath.Dir(o.Directory)); err != nil {
		return report, err
	}
	if !filepath.IsAbs(o.Directory) || filepath.Clean(o.Directory) != o.Directory {
		return report, errors.New("absolute_clean_path_required")
	}
	input, err := os.OpenFile(o.Source, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return report, err
	}
	defer input.Close()
	before, err := input.Stat()
	if err != nil {
		return report, err
	}
	if !privateRegular(before) || before.Size() > o.MaxBytes {
		return report, errors.New("source_not_private_sealed_file")
	}
	if err = noSidecars(o.Source); err != nil {
		return report, err
	}
	if err = os.Mkdir(o.Directory, 0700); err != nil {
		return report, err
	}
	path := filepath.Join(o.Directory, "raw-audit-working.sqlite3")
	output, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return report, err
	}
	hash := sha256.New()
	buffer := make([]byte, 128<<10)
	var copied int64
	for {
		if err = ctx.Err(); err != nil {
			break
		}
		var n int
		n, err = input.Read(buffer)
		if n > 0 {
			copied += int64(n)
			if copied > o.MaxBytes {
				err = errors.New("copy_size_budget")
				break
			}
			if _, e := io.MultiWriter(output, hash).Write(buffer[:n]); e != nil {
				err = e
				break
			}
		}
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}
	}
	if err == nil {
		err = output.Sync()
	}
	closeErr := output.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return report, err
	}
	after, err := input.Stat()
	if err != nil {
		return report, err
	}
	pathInfo, e := os.Lstat(o.Source)
	if e != nil || !stable(before, after) || !stable(before, pathInfo) || copied != before.Size() {
		return report, errors.New("source_changed")
	}
	if err = noSidecars(o.Source); err != nil {
		return report, err
	}
	report.SourceSHA256 = hex.EncodeToString(hash.Sum(nil))
	report.SourceBytes = copied
	info, err := os.Lstat(path)
	if err != nil {
		return report, err
	}
	if !privateRegular(info) || os.SameFile(before, info) {
		return report, errors.New("copy_identity_invalid")
	}
	report.PhysicalBytesBefore = info.Size()
	// Exclusive reference proof is meaningful only for the brand new copy. This
	// command has no path to apply to an existing AO file, even with a flag.
	if o.Apply {
		if err = noOtherReferences(ctx, info); err != nil {
			return report, err
		}
	}
	u := url.URL{Scheme: "file", Path: path}
	dsn := u.String() + "?mode=rw&_pragma=busy_timeout(0)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	var check string
	if err = db.QueryRowContext(ctx, "PRAGMA quick_check(1)").Scan(&check); err != nil || check != "ok" {
		return report, errors.New("copy_quick_check_failed")
	}
	cursor := ""
	for i := 0; i < o.MaxBatches; i++ {
		batch, e := store.FoldRawAuditCopyBatch(ctx, db, cursor, store.RawAuditCopyOptions{BatchRows: o.Rows, MaxRowBytes: o.MaxRowBytes, BatchBytes: o.BatchBytes, Budget: o.BatchBudget, Apply: o.Apply})
		if e != nil {
			return report, e
		}
		report.Scanned += batch.Scanned
		report.Eligible += batch.Eligible
		report.Migrated += batch.Migrated
		report.Retained += batch.Retained
		report.LogicalBytesDelta += batch.LogicalBytesDelta
		if batch.NextCursor == cursor {
			report.Complete = true
			break
		}
		cursor = batch.NextCursor
	}
	if report.Complete {
		report.Status = "complete"
	} else {
		report.Status = "partial"
	}
	if err = db.Close(); err != nil {
		return report, err
	}
	final, e := os.Stat(path)
	if e != nil {
		return report, e
	}
	report.PhysicalBytesAfter = final.Size()
	// The receipt contains only counts/digests. No source ID or payload is logged.
	data, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		return report, e
	}
	receipt, e := os.OpenFile(filepath.Join(o.Directory, "raw-audit-copy-report.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return report, e
	}
	_, e = receipt.Write(data)
	if e == nil {
		e = receipt.Sync()
	}
	ce := receipt.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return report, fmt.Errorf("private_receipt_failed: %w", e)
	}
	return report, nil
}

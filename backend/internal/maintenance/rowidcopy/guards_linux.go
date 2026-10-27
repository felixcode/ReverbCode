//go:build linux

package rowidcopy

import (
 "context"
 "errors"
 "os"
 "path/filepath"
 "strconv"
 "strings"
 "syscall"
)

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


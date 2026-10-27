package store

import (
	"os"
	"syscall"
)

func observeReclaimFiles(path string, device, inode uint64) (out PageReclaimFiles) {
	main, err := os.Stat(path)
	if err != nil {
		return out
	}
	st, ok := main.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Dev) != device || st.Ino != inode {
		return out
	}
	out.MainBytes = main.Size()
	out.MainAllocatedBytes = st.Blocks * 512
	wal, err := os.Stat(path + "-wal")
	if os.IsNotExist(err) {
		out.Known = true
		return out
	}
	if err != nil {
		return out
	}
	ws, ok := wal.Sys().(*syscall.Stat_t)
	if !ok {
		return out
	}
	out.WALPresent = true
	out.WALBytes = wal.Size()
	out.WALAllocatedBytes = ws.Blocks * 512
	out.Known = true
	return out
}

//go:build linux

// Package maintenance serves only a private, same-UID Unix socket. It is never
// attached to the loopback/LAN/mobile router and cannot be forwarded by AO's
// existing gateways. Filesystem ownership and SO_PEERCRED authenticate callers.
package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

type Server struct {
	server   *http.Server
	listener *net.UnixListener
}
type peerKey struct{}

func privateParent(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("maintenance socket path must be canonical absolute")
	}

	mounts, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(mounts), "\n") {
		parts := strings.SplitN(line, " - ", 2)
		if len(parts) != 2 {
			continue
		}
		left, right := strings.Fields(parts[0]), strings.Fields(parts[1])
		if len(left) < 5 || len(right) < 1 {
			continue
		}
		remote := right[0] == "nfs" || right[0] == "nfs4" || right[0] == "cifs" || right[0] == "smb3" || right[0] == "9p" || right[0] == "fuse.sshfs"
		mount := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(left[4])
		if remote && (path == mount || strings.HasPrefix(path, mount+"/")) {
			return errors.New("remote maintenance socket path refused")
		}
	}
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil {
		return err
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode().Perm() != 0700 || int(st.Uid) != os.Getuid() {
		return errors.New("maintenance socket parent must be owned mode 0700")
	}
	for p := parent; p != "/"; p = filepath.Dir(p) {
		i, e := os.Lstat(p)
		if e != nil {
			return e
		}
		if i.Mode()&os.ModeSymlink != 0 {
			return errors.New("maintenance socket parent symlink")
		}
	}
	return nil
}
func peerUID(conn net.Conn) (uint32, error) {
	c, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, errors.New("not Unix")
	}
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var uid uint32
	var inner error
	err = raw.Control(func(fd uintptr) {
		cred, e := syscall.GetsockoptUcred(int(fd), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
		inner = e
		if e == nil {
			uid = cred.Uid
		}
	})
	if err != nil {
		return 0, err
	}
	return uid, inner
}
func Start(path string, s *store.Store, o store.HistoryMaintenanceOptions) (*Server, error) {
	r := store.DefaultPageReclaimOptions()
	r.ReadBudget = min(r.ReadBudget, o.ReadBudget)
	r.WriteBudget = min(r.WriteBudget, o.WriteBudget)
	return StartWithPageReclaim(path, s, o, &r)
}

func StartWithPageReclaim(path string, s *store.Store, o store.HistoryMaintenanceOptions, reclaim *store.PageReclaimOptions) (*Server, error) {
	if reclaim != nil {
		copy := *reclaim
		copy.ReadBudget = min(copy.ReadBudget, o.ReadBudget)
		copy.WriteBudget = min(copy.WriteBudget, o.WriteBudget)
		if copy.Validate() != nil {
			reclaim = nil
		} else {
			reclaim = &copy
		}
	}
	if err := privateParent(path); err != nil {
		return nil, err
	}
	if err := o.Validate(); err != nil {
		return nil, err
	}
	// Existing sockets are never unlinked speculatively: another daemon could own
	// them. Normal Close removes only this listener's inode.
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return nil, errors.New("maintenance socket path already exists or inaccessible")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, err
	}
	listener.SetUnlinkOnClose(true)
	if err = os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: o.ReadBudget, ReadTimeout: o.ReadBudget, WriteTimeout: o.ReadBudget + o.WriteBudget, IdleTimeout: o.ReadBudget, MaxHeaderBytes: 8192}
	srv.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		uid, err := peerUID(c)
		return context.WithValue(ctx, peerKey{}, err == nil && int(uid) == os.Getuid())
	}
	allowed := func(w http.ResponseWriter, r *http.Request) bool {
		ok, _ := r.Context().Value(peerKey{}).(bool)
		if !ok || r.Header.Get("Origin") != "" || r.Header.Get("Forwarded") != "" {
			http.Error(w, "local authenticated maintenance only", http.StatusForbidden)
			return false
		}
		for k := range r.Header {
			if strings.HasPrefix(strings.ToLower(k), "x-forwarded-") {
				http.Error(w, "forwarded maintenance refused", http.StatusForbidden)
				return false
			}
		}
		return true
	}
	mux.HandleFunc("GET /v1/capability", func(w http.ResponseWriter, r *http.Request) {
		if !allowed(w, r) {
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), o.ReadBudget)
		defer cancel()
		cap := s.MaintenanceCapability(ctx)
		cap["maintenanceProtocol"] = 1
		cap["pid"] = os.Getpid()
		cap["retentionHours"] = o.Retention.Hours()
		cap["writeBudgetMS"] = o.WriteBudget.Milliseconds()
		if reclaim == nil {
			cap["pageReclaim"] = map[string]any{"configured": false, "supported": false, "reason": "invalid_reclaim_configuration"}
		} else {
			reclaimCap := s.PageReclaimCapability(ctx)
			reclaimCap["maxPages"] = reclaim.MaxPages
			reclaimCap["minFreePages"] = reclaim.MinFreePages
			reclaimCap["writeBudgetMS"] = reclaim.WriteBudget.Milliseconds()
			cap["pageReclaim"] = reclaimCap
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(cap)
	})
	mux.HandleFunc("POST /v1/history/batch", func(w http.ResponseWriter, r *http.Request) {
		if !allowed(w, r) {
			return
		}
		defer r.Body.Close()
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
		decoder.DisallowUnknownFields()
		var req store.HistoryMaintenanceRequest
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid typed maintenance request", 400)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "one request required", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), o.ReadBudget+o.WriteBudget)
		defer cancel()
		result, err := s.MaintainHistoryBatch(ctx, req, o)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(result)
			return
		}
		json.NewEncoder(w).Encode(result)
	})
	mux.HandleFunc("POST /v1/history/reclaim", func(w http.ResponseWriter, r *http.Request) {
		if !allowed(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if reclaim == nil {
			json.NewEncoder(w).Encode(store.PageReclaimResult{Status: "unsupported", Reason: "invalid_reclaim_configuration", CommitState: "not_started"})
			return
		}
		defer r.Body.Close()
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
		decoder.DisallowUnknownFields()
		var request store.PageReclaimRequest
		if decoder.Decode(&request) != nil || decoder.Decode(new(any)) != io.EOF {
			http.Error(w, "invalid typed reclaim request", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), reclaim.ReadBudget+reclaim.WriteBudget)
		defer cancel()
		result, err := s.ReclaimPages(ctx, request, *reclaim)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(result)
	})
	go func() { _ = srv.Serve(listener) }()
	return &Server{srv, listener}, nil
}
func (s *Server) Close() error { _ = s.server.Close(); return s.listener.Close() }

// StartConfigured is opt-in. The socket's private parent is provisioned by the
// operator, not chmodded/created over an unknown path by the daemon.
func StartConfigured(s *store.Store) (*Server, error) {
	path := os.Getenv("ALBANIA_AO_HISTORY_MAINTENANCE_SOCKET")
	if path == "" {
		return nil, nil
	}
	o := store.DefaultHistoryMaintenanceOptions()
	for name, dst := range map[string]*time.Duration{"RETENTION_HOURS": &o.Retention, "STATEMENT_SECONDS": &o.ReadBudget, "TRANSACTION_SECONDS": &o.WriteBudget} {
		if raw := os.Getenv("ALBANIA_AO_HISTORY_" + name); raw != "" {
			v, e := strconv.ParseFloat(raw, 64)
			if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > float64(1<<30) {
				return nil, errors.New("invalid maintenance duration")
			}
			unit := time.Second
			if name == "RETENTION_HOURS" {
				unit = time.Hour
			}
			if v > float64(math.MaxInt64)/float64(unit) {
				return nil, errors.New("maintenance duration overflow")
			}
			*dst = time.Duration(v * float64(unit))
		}
	}
	for name, dst := range map[string]*int64{"BATCH_BYTES": &o.BatchBytes, "MAX_ROW_BYTES": &o.MaxRowBytes} {
		if raw := os.Getenv("ALBANIA_AO_HISTORY_" + name); raw != "" {
			v, e := strconv.ParseInt(raw, 10, 64)
			if e != nil || v <= 0 {
				return nil, errors.New("invalid maintenance size")
			}
			*dst = v
		}
	}
	if raw := os.Getenv("ALBANIA_AO_HISTORY_BATCH_ROWS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n <= 0 {
			return nil, errors.New("invalid maintenance rows")
		}
		o.BatchRows = n
	}
	if raw := os.Getenv("ALBANIA_AO_HISTORY_GC_BATCH_ROWS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 1000 { return nil, errors.New("invalid GC maintenance rows") }
		o.GCBatchRows = n
	}
	reclaim, reclaimErr := configuredPageReclaimOptions(o)
	if reclaimErr != nil {
		return StartWithPageReclaim(path, s, o, nil)
	}
	return StartWithPageReclaim(path, s, o, &reclaim)
}

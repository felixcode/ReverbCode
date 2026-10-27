package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	modernsqlite "modernc.org/sqlite"
)

type snapshotSQLMetric struct {
	Calls       int64 `json:"calls"`
	Nanoseconds int64 `json:"nanoseconds"`
}
type snapshotSQLProfile struct {
	mu      sync.Mutex
	metrics map[string]snapshotSQLMetric
	queries map[string]string
	args    map[string][]driver.NamedValue
}

func snapshotSQLName(q string) string {
	switch {
	case strings.HasPrefix(q, "EXPLAIN"):
		return "explain"
	case strings.Contains(q, "ao_history_retention"):
		return "marker_probe"
	case strings.Contains(q, "SELECT is_terminated"):
		return "owner_status"
	case strings.Contains(q, "FROM review_run"):
		return "review_active"
	case strings.Contains(q, "FROM agent_switches"):
		return "switch_pending"
	case strings.Contains(q, "FROM conversation_branches"):
		return "owner_branches"
	case strings.Contains(q, "FROM conversations"):
		return "owner_conversation"
	case strings.Contains(q, "FROM conversation_activities a JOIN conversation_turns"):
		return "owner_pending_activity"
	case strings.Contains(q, "FROM conversation_activities"):
		return "turn_pending_activity"
	case strings.Contains(q, "FROM conversation_turns WHERE handled_by_session_id"):
		return "owner_live_turn"
	case strings.Contains(q, "FROM conversation_turns WHERE conversation_id=? AND handled_by_session_id"):
		return "native_turn_lookup"
	case strings.Contains(q, "FROM conversation_turns WHERE conversation_id"):
		return "conversation_live_turn"
	case strings.Contains(q, "FROM conversation_turns WHERE id"):
		return "turn_metadata"
	case strings.Contains(q, "conversation_provider_events") && strings.Contains(q, "octet_length"):
		return "event_size"
	case strings.Contains(q, "conversation_provider_events") && strings.HasPrefix(q, "SELECT 1"):
		return "event_cas"
	case strings.Contains(q, "conversation_provider_events") && strings.Contains(q, "payload_json"):
		return "event_body"
	case strings.Contains(q, "conversation_provider_events"):
		return "event_keys_or_metadata"
	default:
		return "other"
	}
}
func (p *snapshotSQLProfile) add(name string, n time.Duration, call bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	m := p.metrics[name]
	if call {
		m.Calls++
	}
	m.Nanoseconds += int64(n)
	p.metrics[name] = m
}

type snapshotProfileDriver struct {
	p   *snapshotSQLProfile
	dsn string
}

func (d snapshotProfileDriver) Open(string) (driver.Conn, error) {
	c, e := (&modernsqlite.Driver{}).Open(d.dsn)
	if e != nil {
		return nil, e
	}
	return &snapshotProfileConn{Conn: c, p: d.p}, nil
}

type snapshotProfileConn struct {
	driver.Conn
	p *snapshotSQLProfile
}

func (c *snapshotProfileConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	name := snapshotSQLName(q)
	start := time.Now()
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	c.p.add(name, time.Since(start), true)
	c.p.mu.Lock()
	if _, ok := c.p.queries[name]; !ok {
		c.p.queries[name] = q
		c.p.args[name] = append([]driver.NamedValue{}, args...)
	}
	c.p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &snapshotProfileRows{Rows: rows, p: c.p, name: name}, nil
}
func (c *snapshotProfileConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("profile_exec_forbidden")
}
func (c *snapshotProfileConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	if !o.ReadOnly {
		return nil, errors.New("profile_readonly_required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.Conn.Begin()
}

type snapshotProfileRows struct {
	driver.Rows
	p    *snapshotSQLProfile
	name string
}

func (r *snapshotProfileRows) Next(v []driver.Value) error {
	s := time.Now()
	e := r.Rows.Next(v)
	r.p.add(r.name, time.Since(s), false)
	return e
}
func (r *snapshotProfileRows) Close() error {
	s := time.Now()
	e := r.Rows.Close()
	r.p.add(r.name, time.Since(s), false)
	return e
}

func snapshotPlanSHA(plans []maintenancePrepared) string {
	h := sha256.New()
	e := json.NewEncoder(h)
	for _, p := range plans {
		proof := []any{}
		if p.proof != nil {
			proof = []any{p.proof.native, p.proof.hasBody, p.proof.hasRetention}
		}
		audits := [][]any{}
		for _, a := range p.audits {
			audits = append(audits, []any{a.kind, a.id, a.conversation, a.semantic, a.prompt, a.originalSHA, a.at, a.cutoff, a.fields, a.bytes})
		}
		_ = e.Encode([]any{p.id, p.row, p.cas, p.bodyColumns, p.builtin, p.skip, proof, p.updates, p.fields, audits, p.inlineExpired, p.noRawBody, p.size})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Opt-in diagnostic only: the reviewed runner binds the historical sealed file
// readonly to this exact path, with no network and no writable AO directories.
func TestSnapshotPrepareProfileReadonly(t *testing.T) {
	if os.Getenv("AO_SNAPSHOT_PROFILE_SOURCE") == "" {
		t.Skip("readonly_static_profile_opt_in")
	}
	path := os.Getenv("AO_SNAPSHOT_PROFILE_SOURCE")
	if path != "/snapshot/historical.sqlite" {
		t.Fatal("profile_static_mount_required")
	}
	before, err := os.Stat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode().Perm() != 0600 {
		t.Fatal("profile_source_metadata")
	}
	asOf, err := time.Parse(time.RFC3339Nano, os.Getenv("AO_SNAPSHOT_PROFILE_AS_OF"))
	if err != nil {
		t.Fatal("profile_clock")
	}
	u := url.URL{Scheme: "file", Path: path}
	dsn := u.String() + "?mode=ro&immutable=1"
	p := &snapshotSQLProfile{metrics: map[string]snapshotSQLMetric{}, queries: map[string]string{}, args: map[string][]driver.NamedValue{}}
	sql.Register("snapshot-profile-1", snapshotProfileDriver{p, dsn})
	db, err := sql.Open("snapshot-profile-1", "")
	if err != nil {
		t.Fatal("profile_open")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var low, high int64
	if err = db.QueryRowContext(ctx, `SELECT min(id),max(id) FROM conversation_provider_events`).Scan(&low, &high); err != nil {
		t.Fatal("profile_bounds")
	}
	starts := []struct {
		name  string
		start int64
	}{{"first", low - 1}, {"body_early", 200000}, {"body_current", 800000}, {"middle", (low + high) / 2}, {"tail", high - 33}}
	for _, window := range starts {
		rows, err := db.QueryContext(ctx, `SELECT id FROM conversation_provider_events WHERE id>? ORDER BY id LIMIT 32`, window.start)
		if err != nil {
			t.Fatal("profile_window")
		}
		ids := []any{}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				t.Fatal("profile_ids")
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal("profile_ids")
		}
		p.mu.Lock()
		p.metrics = map[string]snapshotSQLMetric{}
		p.queries = map[string]string{}
		p.args = map[string][]driver.NamedValue{}
		p.mu.Unlock()
		tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
		if err != nil {
			t.Fatal("profile_begin")
		}
		start := time.Now()
		plans, err := prepareMaintenanceRows(ctx, tx, maintenanceStages["raw"], "raw", "id", ids, DefaultHistoryMaintenanceOptions(), asOf, asOf.Add(-47*time.Hour), true)
		elapsed := time.Since(start)
		tx.Rollback()
		if err != nil {
			t.Fatal("profile_prepare_failed")
		}
		retained := map[string]int{}
		body := 0
		for _, plan := range plans {
			if plan.skip != "" {
				retained[plan.skip]++
			}
			if plan.proof != nil && plan.proof.hasBody {
				body++
			}
		}
		type metric struct {
			Name        string `json:"template"`
			Calls       int64  `json:"calls"`
			Nanoseconds int64  `json:"nanoseconds"`
			PlanSHA     string `json:"explain_sha256"`
			Scan        int    `json:"scans"`
			Search      int    `json:"searches"`
		}
		values := []metric{}
		p.mu.Lock()
		saved := map[string]snapshotSQLMetric{}
		for n, m := range p.metrics {
			saved[n] = m
		}
		queries := p.queries
		arguments := p.args
		p.mu.Unlock()
		for name, m := range saved {
			args := []any{}
			for _, a := range arguments[name] {
				args = append(args, a.Value)
			}
			explain, e := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+queries[name], args...)
			if e != nil {
				t.Fatal("profile_explain")
			}
			details := []string{}
			scans, search := 0, 0
			for explain.Next() {
				var a, b, c int
				var detail string
				if e = explain.Scan(&a, &b, &c, &detail); e != nil {
					t.Fatal("profile_plan")
				}
				details = append(details, detail)
				scans += boolInt(strings.HasPrefix(detail, "SCAN "))
				search += boolInt(strings.HasPrefix(detail, "SEARCH "))
			}
			explain.Close()
			encoded, _ := json.Marshal(details)
			sum := sha256.Sum256(encoded)
			values = append(values, metric{name, m.Calls, m.Nanoseconds, hex.EncodeToString(sum[:]), scans, search})
		}
		sort.Slice(values, func(i, j int) bool { return values[i].Nanoseconds > values[j].Nanoseconds })
		out, _ := json.Marshal(map[string]any{"window": window.name, "input_rows": len(ids), "prepared_rows": len(plans), "body_rows": body, "retained": retained, "prepare_nanoseconds": int64(elapsed), "plan_sha256": snapshotPlanSHA(plans), "queries": values})
		t.Log(string(out))
	}
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || before.ModTime() != after.ModTime() {
		t.Fatal("profile_source_changed")
	}
	t.Log(fmt.Sprintf("source_reference_sha256=%s verified_by_parent_seal=true local_full_rehash=false", os.Getenv("AO_SNAPSHOT_PROFILE_SOURCE_SHA256")))
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

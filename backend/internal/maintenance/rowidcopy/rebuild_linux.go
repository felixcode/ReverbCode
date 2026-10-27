//go:build linux

// Package rowidcopy rebuilds a sealed snapshot into a newly created private
// candidate. It is deliberately separate from the daemon and has no publish path.
package rowidcopy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

type Options struct {
	Source, Directory, SourceSHA256 string
	Apply                           bool
	MaxBytes                        int64
	Budget                          time.Duration
}
type TableReport struct {
	Name         string `json:"name"`
	Rows         int64  `json:"rows"`
	WithoutRowID bool   `json:"without_rowid"`
	RowIDSHA256  string `json:"rowid_sha256,omitempty"`
}
type Report struct {
	Status            string        `json:"status"`
	Apply             bool          `json:"apply"`
	SourceSHA256      string        `json:"source_sha256"`
	CandidateSHA256   string        `json:"candidate_sha256,omitempty"`
	SourceBytes       int64         `json:"source_bytes"`
	CandidateBytes    int64         `json:"candidate_bytes"`
	AutoVacuum        int           `json:"auto_vacuum"`
	SchemaObjects     int           `json:"schema_objects"`
	Tables            []TableReport `json:"tables"`
	SourceReadOnlyFDs bool          `json:"source_read_only_fds"`
}
type object struct{ kind, name, table, ddl string }
type table struct {
	name, rowid           string
	columns, writable, pk []string
	without               bool
}

func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func scalar(ctx context.Context, c *sql.Conn, q string) (v int64, err error) {
	err = c.QueryRowContext(ctx, q).Scan(&v)
	return
}

func objects(ctx context.Context, c *sql.Conn, schema string) (out []object, err error) {
	rows, err := c.QueryContext(ctx, `SELECT type,name,tbl_name,coalesce(sql,'') FROM `+schema+`.sqlite_schema ORDER BY rowid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var x object
		if err = rows.Scan(&x.kind, &x.name, &x.table, &x.ddl); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func inventory(ctx context.Context, c *sql.Conn, objs []object) ([]table, error) {
	kind := map[string]string{}
	wr := map[string]bool{}
	rows, err := c.QueryContext(ctx, `PRAGMA sealed.table_list`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var schema, name, t string
		var n, w, s int
		if err = rows.Scan(&schema, &name, &t, &n, &w, &s); err != nil {
			rows.Close()
			return nil, err
		}
		kind[name] = t
		wr[name] = w != 0
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var tables []table
	for _, obj := range objs {
		switch obj.kind {
		case "table", "index", "view", "trigger":
		default:
			return nil, errors.New("unsupported_schema_object")
		}
		if obj.kind != "table" {
			continue
		}
		if strings.HasPrefix(strings.ToLower(obj.name), "sqlite_") && obj.name != "sqlite_sequence" {
			return nil, errors.New("unsupported_internal_table")
		}
		if kind[obj.name] != "table" {
			return nil, errors.New("virtual_shadow_or_unknown_table")
		}
		if obj.ddl == "" {
			return nil, errors.New("missing_table_schema")
		}
		t := table{name: obj.name, without: wr[obj.name]}
		names := map[string]bool{}
		pk := map[int]string{}
		r, e := c.QueryContext(ctx, `PRAGMA sealed.table_xinfo(`+quote(obj.name)+`)`)
		if e != nil {
			return nil, e
		}
		for r.Next() {
			var cid, notnull, p, hidden int
			var name, typ string
			var def any
			if e = r.Scan(&cid, &name, &typ, &notnull, &def, &p, &hidden); e != nil {
				r.Close()
				return nil, e
			}
			names[strings.ToLower(name)] = true
			t.columns = append(t.columns, name)
			if hidden == 0 {
				t.writable = append(t.writable, name)
			} else if hidden != 2 && hidden != 3 {
				r.Close()
				return nil, errors.New("unsupported_hidden_column")
			}
			if p > 0 {
				pk[p] = name
			}
		}
		e = r.Err()
		r.Close()
		if e != nil {
			return nil, e
		}
		if len(t.writable) == 0 {
			return nil, errors.New("no_writable_columns")
		}
		for i := 1; i <= len(pk); i++ {
			name, ok := pk[i]
			if !ok {
				return nil, errors.New("unknown_primary_key")
			}
			t.pk = append(t.pk, name)
		}
		if t.without {
			if len(t.pk) == 0 {
				return nil, errors.New("without_rowid_missing_key")
			}
		} else {
			for _, alias := range []string{"rowid", "_rowid_", "oid"} {
				if !names[alias] {
					t.rowid = alias
					break
				}
			}
			if t.rowid == "" {
				return nil, errors.New("all_hidden_rowid_names_shadowed")
			}
		}
		tables = append(tables, t)
	}
	return tables, nil
}

// verifyValues compares inside SQLite, including storage class and binary text
// equality. No payload crosses database/sql or is serialized into the report.
func verifyValues(ctx context.Context, c *sql.Conn, t table) error {
	n, err := scalar(ctx, c, `SELECT count(*) FROM sealed.`+quote(t.name))
	if err != nil {
		return err
	}
	m, err := scalar(ctx, c, `SELECT count(*) FROM main.`+quote(t.name))
	if err != nil {
		return err
	}
	if n != m {
		return errors.New("row_count_mismatch")
	}
	var keys, changes []string
	if !t.without {
		keys = []string{`d.` + quote(t.rowid) + ` IS s.` + quote(t.rowid)}
	} else {
		for _, k := range t.pk {
			keys = append(keys, `d.`+quote(k)+` IS s.`+quote(k)+` COLLATE BINARY`)
		}
	}
	for _, name := range t.columns {
		a := `d.` + quote(name)
		b := `s.` + quote(name)
		changes = append(changes, `typeof(`+a+`) != typeof(`+b+`) OR NOT (`+a+` IS `+b+` COLLATE BINARY)`)
	}
	q := `SELECT EXISTS(SELECT 1 FROM sealed.` + quote(t.name) + ` s WHERE NOT EXISTS(SELECT 1 FROM main.` + quote(t.name) + ` d WHERE ` + strings.Join(keys, " AND ") + ` AND NOT (` + strings.Join(changes, " OR ") + `)))`
	bad, err := scalar(ctx, c, q)
	if err != nil {
		return err
	}
	if bad != 0 {
		return errors.New("rowid_or_typed_value_mismatch")
	}
	return nil
}
func rowidDigest(ctx context.Context, c *sql.Conn, schema string, t table) (string, error) {
	rows, err := c.QueryContext(ctx, `SELECT `+quote(t.rowid)+` FROM `+schema+`.`+quote(t.name)+` ORDER BY `+quote(t.rowid))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	h := sha256.New()
	var b [8]byte
	for rows.Next() {
		var n int64
		if err = rows.Scan(&n); err != nil {
			return "", err
		}
		binary.BigEndian.PutUint64(b[:], uint64(n))
		h.Write(b[:])
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func sourceFDsReadOnly(info os.FileInfo) bool {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return false
	}
	seen := false
	for _, e := range entries {
		p := filepath.Join("/proc/self/fd", e.Name())
		st, err := os.Stat(p)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return false
		}
		if !os.SameFile(info, st) {
			continue
		}
		seen = true
		f, err := os.Open(filepath.Join("/proc/self/fdinfo", e.Name()))
		if err != nil {
			return false
		}
		b, err := io.ReadAll(f)
		f.Close()
		if err != nil {
			return false
		}
		var flags uint64
		found := false
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "flags:") {
				_, err = fmt.Sscanf(line, "flags: %o", &flags)
				found = err == nil
				break
			}
		}
		if !found || flags&syscall.O_ACCMODE != syscall.O_RDONLY {
			return false
		}
	}
	return seen
}
func syncDir(path string) error {
	d, e := os.Open(path)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

// Run never mutates Source and never accepts a candidate file path. Apply only
// creates fixed candidate.sqlite3 in Directory, which must not already exist.
func Run(parent context.Context, o Options) (report Report, err error) {
	report.Apply = o.Apply
	if o.MaxBytes <= 0 || o.Budget <= 0 || len(o.SourceSHA256) != 64 {
		return report, errors.New("invalid_budget_or_expected_digest")
	}
	if _, err = hex.DecodeString(o.SourceSHA256); err != nil {
		return report, errors.New("invalid_expected_digest")
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
		return report, errors.New("absolute_clean_directory_required")
	}
	in, err := os.OpenFile(o.Source, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return report, err
	}
	defer in.Close()
	before, err := in.Stat()
	if err != nil {
		return report, err
	}
	if !privateRegular(before) || before.Size() > o.MaxBytes {
		return report, errors.New("source_not_private_sealed_file")
	}
	if err = noSidecars(o.Source); err != nil {
		return report, err
	}
	h := sha256.New()
	buf := make([]byte, 128<<10)
	var hashed int64
	for {
		if err = ctx.Err(); err != nil {
			return report, err
		}
		n, e := in.Read(buf)
		if n > 0 {
			hashed += int64(n)
			if hashed > o.MaxBytes || hashed > before.Size() {
				return report, errors.New("source_grew_during_hash")
			}
			h.Write(buf[:n])
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return report, e
		}
	}
	report.SourceSHA256 = hex.EncodeToString(h.Sum(nil))
	report.SourceBytes = before.Size()
	if report.SourceSHA256 != strings.ToLower(o.SourceSHA256) {
		return report, errors.New("source_digest_mismatch")
	}
	after, err := in.Stat()
	if err != nil || !stable(before, after) {
		return report, errors.New("source_changed")
	}
	// ATTACH requires the main connection's encoding to already match. Read only
	// the fixed SQLite header, after verifying the complete source digest.
	var header [100]byte
	if _, err = in.ReadAt(header[:], 0); err != nil || string(header[:16]) != "SQLite format 3\x00" {
		return report, errors.New("invalid_sqlite_header")
	}
	headerEncoding := map[uint32]string{1: "UTF-8", 2: "UTF-16le", 3: "UTF-16be"}[binary.BigEndian.Uint32(header[56:60])]
	if headerEncoding == "" {
		return report, errors.New("unknown_sqlite_header_encoding")
	}
	// Existing snapshot provenance plus a fixed expected hash is required; an
	// arbitrary live source or unreadable process-reference state is not accepted.
	if err = noOtherReferences(ctx, before); err != nil {
		return report, err
	}
	if err = os.Mkdir(o.Directory, 0700); err != nil {
		return report, err
	}
	if err = os.Chmod(o.Directory, 0700); err != nil {
		return report, err
	}
	dest := filepath.Join(o.Directory, "candidate.sqlite3")
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return report, err
	}
	if err = f.Close(); err != nil {
		return report, err
	}
	info, err := os.Lstat(dest)
	if err != nil || !privateRegular(info) || os.SameFile(before, info) {
		return report, errors.New("candidate_identity_invalid")
	}
	if err = noOtherReferences(ctx, info); err != nil {
		return report, err
	}
	u := url.URL{Scheme: "file", Path: dest}
	db, err := sql.Open("sqlite", u.String()+"?mode=rw&_pragma=busy_timeout(0)")
	if err != nil {
		return report, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	c, err := db.Conn(ctx)
	if err != nil {
		return report, err
	}
	defer c.Close()
	if _, err = c.ExecContext(ctx, `PRAGMA main.encoding='`+headerEncoding+`'`); err != nil {
		return report, err
	}
	origin := url.URL{Scheme: "file", Path: o.Source}
	if _, err = c.ExecContext(ctx, `ATTACH DATABASE ? AS sealed`, origin.String()+"?mode=ro&immutable=1"); err != nil {
		return report, err
	}
	report.SourceReadOnlyFDs = sourceFDsReadOnly(before)
	if !report.SourceReadOnlyFDs {
		return report, errors.New("source_fd_not_proven_read_only")
	}
	objs, err := objects(ctx, c, "sealed")
	if err != nil {
		return report, err
	}
	tables, err := inventory(ctx, c, objs)
	if err != nil {
		return report, err
	}
	report.SchemaObjects = len(objs)
	if !o.Apply {
		report.Status = "preview"
		for _, t := range tables {
			report.Tables = append(report.Tables, TableReport{Name: t.name, WithoutRowID: t.without})
		}
		return report, nil
	}
	// Keep any external-sort spill on the candidate's own local filesystem. This
	// process-only maintenance tool is never embedded in the AO daemon.
	scratch := filepath.Join(o.Directory, "sqlite-tmp")
	if err = os.Mkdir(scratch, 0700); err != nil {
		return report, err
	}
	if _, err = c.ExecContext(ctx, `PRAGMA temp_store_directory='`+strings.ReplaceAll(scratch, "'", "''")+`'`); err != nil {
		return report, errors.New("private_temp_directory_not_supported")
	}
	var tempDir string
	if err = c.QueryRowContext(ctx, `PRAGMA temp_store_directory`).Scan(&tempDir); err != nil || tempDir != scratch {
		return report, errors.New("private_temp_directory_not_proven")
	}
	var pageSize, userVersion, appID int64
	var encoding string
	for _, p := range []struct {
		name  string
		value *int64
	}{{"page_size", &pageSize}, {"user_version", &userVersion}, {"application_id", &appID}} {
		if err = c.QueryRowContext(ctx, `PRAGMA sealed.`+p.name).Scan(p.value); err != nil {
			return report, err
		}
	}
	if err = c.QueryRowContext(ctx, `PRAGMA sealed.encoding`).Scan(&encoding); err != nil {
		return report, err
	}
	if encoding != "UTF-8" && encoding != "UTF-16le" && encoding != "UTF-16be" {
		return report, errors.New("unknown_encoding")
	}
	if pageSize < 512 || pageSize > 65536 || pageSize&(pageSize-1) != 0 {
		return report, errors.New("invalid_page_size")
	}
	for _, q := range []string{`PRAGMA foreign_keys=OFF`, `PRAGMA main.page_size=` + fmt.Sprint(pageSize), `PRAGMA main.encoding='` + encoding + `'`, `PRAGMA main.auto_vacuum=INCREMENTAL`, `PRAGMA main.max_page_count=` + fmt.Sprint(o.MaxBytes/pageSize), `PRAGMA main.journal_mode=DELETE`, `PRAGMA main.synchronous=FULL`, `PRAGMA main.cell_size_check=ON`} {
		if _, err = c.ExecContext(ctx, q); err != nil {
			return report, err
		}
	}
	if _, err = c.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		return report, err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = c.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()
	for _, obj := range objs {
		if obj.kind == "table" && obj.name != "sqlite_sequence" {
			if _, err = c.ExecContext(ctx, obj.ddl); err != nil {
				return report, errors.New("table_schema_not_supported")
			}
		}
	}
	// sqlite_sequence is populated by autoincrement inserts, then replaced by the
	// exact source content, including gaps, duplicate names and its hidden rowids.
	for _, t := range tables {
		if t.name == "sqlite_sequence" {
			continue
		}
		cols := append([]string(nil), t.writable...)
		if !t.without {
			cols = append([]string{t.rowid}, cols...)
		}
		for i := range cols {
			cols[i] = quote(cols[i])
		}
		q := `INSERT INTO main.` + quote(t.name) + ` (` + strings.Join(cols, ",") + `) SELECT ` + strings.Join(cols, ",") + ` FROM sealed.` + quote(t.name)
		if !t.without {
			q += ` ORDER BY ` + quote(t.rowid)
		}
		if _, err = c.ExecContext(ctx, q); err != nil {
			return report, errors.New("table_copy_failed")
		}
	}
	for _, t := range tables {
		if t.name == "sqlite_sequence" {
			if _, err = c.ExecContext(ctx, `DELETE FROM main.sqlite_sequence; INSERT INTO main.sqlite_sequence(rowid,name,seq) SELECT rowid,name,seq FROM sealed.sqlite_sequence`); err != nil {
				return report, errors.New("sequence_copy_failed")
			}
		}
	}
	for _, kind := range []string{"index", "view", "trigger"} {
		for _, obj := range objs {
			if obj.kind == kind && obj.ddl != "" {
				if _, err = c.ExecContext(ctx, obj.ddl); err != nil {
					return report, errors.New("schema_object_not_supported")
				}
			}
		}
	}
	for _, q := range []string{`PRAGMA main.user_version=` + fmt.Sprint(userVersion), `PRAGMA main.application_id=` + fmt.Sprint(appID)} {
		if _, err = c.ExecContext(ctx, q); err != nil {
			return report, err
		}
	}
	var schemaMismatch int64
	q := `SELECT EXISTS(SELECT type,name,tbl_name,coalesce(sql,'') FROM sealed.sqlite_schema EXCEPT SELECT type,name,tbl_name,coalesce(sql,'') FROM main.sqlite_schema) OR EXISTS(SELECT type,name,tbl_name,coalesce(sql,'') FROM main.sqlite_schema EXCEPT SELECT type,name,tbl_name,coalesce(sql,'') FROM sealed.sqlite_schema)`
	if err = c.QueryRowContext(ctx, q).Scan(&schemaMismatch); err != nil || schemaMismatch != 0 {
		return report, errors.New("schema_mismatch")
	}
	for _, t := range tables {
		if err = verifyValues(ctx, c, t); err != nil {
			return report, err
		}
		r := TableReport{Name: t.name, WithoutRowID: t.without}
		r.Rows, err = scalar(ctx, c, `SELECT count(*) FROM main.`+quote(t.name))
		if err != nil {
			return report, err
		}
		if !t.without {
			r.RowIDSHA256, err = rowidDigest(ctx, c, "sealed", t)
			if err != nil {
				return report, err
			}
			other, e := rowidDigest(ctx, c, "main", t)
			if e != nil {
				return report, e
			}
			if other != r.RowIDSHA256 {
				return report, errors.New("rowid_digest_mismatch")
			}
		}
		report.Tables = append(report.Tables, r)
	}
	var check string
	if err = c.QueryRowContext(ctx, `PRAGMA main.integrity_check(1)`).Scan(&check); err != nil || check != "ok" {
		return report, errors.New("candidate_integrity_check_failed")
	}
	fk, err := c.QueryContext(ctx, `PRAGMA main.foreign_key_check`)
	if err != nil {
		return report, err
	}
	bad := fk.Next()
	fkErr := fk.Err()
	fk.Close()
	if bad || fkErr != nil {
		return report, errors.New("candidate_foreign_key_check_failed")
	}
	if err = c.QueryRowContext(ctx, `PRAGMA main.auto_vacuum`).Scan(&report.AutoVacuum); err != nil || report.AutoVacuum != 2 {
		return report, errors.New("incremental_mode_not_set")
	}
	after, err = in.Stat()
	pathAfter, e := os.Lstat(o.Source)
	if err != nil || e != nil || !stable(before, after) || !stable(before, pathAfter) {
		return report, errors.New("source_changed")
	}
	if err = noSidecars(o.Source); err != nil {
		return report, err
	}
	if !sourceFDsReadOnly(before) {
		return report, errors.New("source_read_only_proof_changed")
	}
	if _, err = c.ExecContext(ctx, `COMMIT`); err != nil {
		return report, err
	}
	committed = true
	if err = c.Close(); err != nil {
		return report, err
	}
	if err = db.Close(); err != nil {
		return report, err
	}
	if err = noSidecars(dest); err != nil {
		return report, err
	}
	candidate, err := os.Open(dest)
	if err != nil {
		return report, err
	}
	err = candidate.Sync()
	candidate.Close()
	if err != nil {
		return report, err
	}
	final, e := os.Lstat(dest)
	if e != nil || !privateRegular(final) || !os.SameFile(info, final) || final.Size() > o.MaxBytes {
		return report, errors.New("candidate_identity_or_size_changed")
	}
	report.CandidateBytes = final.Size()
	sealed, err := os.OpenFile(dest, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return report, err
	}
	h = sha256.New()
	for {
		if err = ctx.Err(); err != nil {
			sealed.Close()
			return report, err
		}
		n, e := sealed.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			sealed.Close()
			return report, e
		}
	}
	last, e := sealed.Stat()
	sealed.Close()
	if e != nil || !stable(final, last) {
		return report, errors.New("candidate_changed_during_seal")
	}
	report.CandidateSHA256 = hex.EncodeToString(h.Sum(nil))
	report.Status = "complete_unpublished"
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, err
	}
	receipt, err := os.OpenFile(filepath.Join(o.Directory, "rowid-rebuild-report.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return report, err
	}
	_, err = receipt.Write(data)
	if err == nil {
		err = receipt.Sync()
	}
	closeErr := receipt.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return report, err
	}
	if err = syncDir(o.Directory); err != nil {
		return report, err
	}
	return report, nil
}

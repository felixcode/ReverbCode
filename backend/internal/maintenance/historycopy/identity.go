//go:build linux

package historycopy

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// TableIdentity is a versioned comparison interface for a later rebuild. Only
// identifiers and types enter its streaming digest; no row values are emitted.
type TableIdentity struct {
	Table              string   `json:"table"`
	Rows               int64    `json:"rows"`
	PrimaryKey         []string `json:"primary_key"`
	WithoutRowID       bool     `json:"without_rowid"`
	IntegerAlias       bool     `json:"integer_primary_key_alias"`
	IdentitySHA256     string   `json:"identity_sha256,omitempty"`
	RowIDMappingSHA256 string   `json:"rowid_mapping_sha256,omitempty"`
	UnknownReason      string   `json:"unknown_reason,omitempty"`
}

func quoted(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// CensusIdentity reads one snapshot and retains TEXT/composite-PK to implicit
// ROWID mapping. A no-PK/virtual table is explicitly unknown for rebuild proof.
// It never treats a digest alone as authorization to delete/rebuild anything.
func CensusIdentity(ctx context.Context, db *sql.DB) ([]TableIdentity, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	tables, err := tx.QueryContext(ctx, `PRAGMA table_list`)
	if err != nil {
		return nil, err
	}
	type meta struct {
		name, kind string
		without    bool
	}
	list := []meta{}
	for tables.Next() {
		var schema, name, kind string
		var cols, without, strict int
		if err = tables.Scan(&schema, &name, &kind, &cols, &without, &strict); err != nil {
			break
		}
		if schema == "main" && name != "sqlite_schema" {
			list = append(list, meta{name, kind, without == 1})
		}
	}
	if err == nil {
		err = tables.Err()
	}
	tables.Close()
	if err != nil {
		return nil, err
	}
	sort.Slice(list, func(i, j int) bool { return list[i].name < list[j].name })
	out := []TableIdentity{}
	for _, m := range list {
		r := TableIdentity{Table: m.name, PrimaryKey: []string{}, WithoutRowID: m.without}
		if m.kind != "table" {
			r.UnknownReason = "virtual_or_shadow_table"
			out = append(out, r)
			continue
		}
		cols, e := tx.QueryContext(ctx, `PRAGMA table_info(`+quoted(m.name)+`)`)
		if e != nil {
			return out, e
		}
		type key struct {
			order     int
			name, typ string
		}
		keys := []key{}
		all := map[string]bool{}
		for cols.Next() {
			var n, nn, pk int
			var name, typ string
			var def any
			if e = cols.Scan(&n, &name, &typ, &nn, &def, &pk); e != nil {
				break
			}
			all[strings.ToLower(name)] = true
			if pk > 0 {
				keys = append(keys, key{pk, name, typ})
			}
		}
		if e == nil {
			e = cols.Err()
		}
		cols.Close()
		if e != nil {
			return out, e
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].order < keys[j].order })
		for _, k := range keys {
			r.PrimaryKey = append(r.PrimaryKey, k.name)
		}
		if len(keys) == 1 && strings.EqualFold(keys[0].typ, "INTEGER") && !m.without {
			// INTEGER PRIMARY KEY DESC can have a separate PK index and is not an alias.
			idx, e := tx.QueryContext(ctx, `PRAGMA index_list(`+quoted(m.name)+`)`)
			if e != nil {
				return out, e
			}
			r.IntegerAlias = true
			for idx.Next() {
				var seq, unique, partial int
				var name, origin string
				if e = idx.Scan(&seq, &name, &unique, &origin, &partial); e != nil {
					break
				}
				if origin == "pk" {
					r.IntegerAlias = false
				}
			}
			if e == nil {
				e = idx.Err()
			}
			idx.Close()
			if e != nil {
				return out, e
			}
		}
		rowID := ""
		if !m.without {
			for _, alias := range []string{"rowid", "_rowid_", "oid"} {
				if !all[alias] {
					rowID = alias
					break
				}
			}
		}
		if len(keys) == 0 {
			r.UnknownReason = "no_declared_stable_key"
		}
		if !m.without && rowID == "" {
			r.UnknownReason = "rowid_aliases_shadowed"
		}
		selected, order := []string{}, []string{}
		for _, k := range keys {
			selected = append(selected, quoted(k.name))
			order = append(order, quoted(k.name))
		}
		if rowID != "" {
			selected = append([]string{rowID}, selected...)
			order = []string{rowID}
		}
		if len(selected) == 0 {
			if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM `+quoted(m.name)).Scan(&r.Rows); e != nil {
				return out, e
			}
			out = append(out, r)
			continue
		}
		rows, e := tx.QueryContext(ctx, `SELECT `+strings.Join(selected, ",")+` FROM `+quoted(m.name)+` ORDER BY `+strings.Join(order, ","))
		if e != nil {
			return out, e
		}
		h := sha256.New()
		mapping := sha256.New()
		for rows.Next() {
			values := make([]any, len(selected))
			ptrs := make([]any, len(selected))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if e = rows.Scan(ptrs...); e != nil {
				break
			}
			// Type-tagged, JSON-framed tuples distinguish null, text, integer and blob.
			tuple := make([]any, len(values))
			for i, v := range values {
				switch x := v.(type) {
				case nil:
					tuple[i] = []any{"null", nil}
				case int64:
					tuple[i] = []any{"integer", fmt.Sprint(x)}
				case string:
					tuple[i] = []any{"text", x}
				case []byte:
					tuple[i] = []any{"blob", hex.EncodeToString(x)}
				case float64:
					tuple[i] = []any{"real", fmt.Sprintf("%x", x)}
				default:
					e = errors.New("identity_storage_type_unknown")
				}
			}
			if e != nil {
				break
			}
			keyTuple := tuple
			if rowID != "" {
				keyTuple = tuple[1:]
			}
			encoded, marshalErr := json.Marshal(keyTuple)
			if marshalErr != nil {
				e = marshalErr
				break
			}
			h.Write(encoded)
			h.Write([]byte{'\n'})
			encoded, marshalErr = json.Marshal(tuple)
			if marshalErr != nil {
				e = marshalErr
				break
			}
			mapping.Write(encoded)
			mapping.Write([]byte{'\n'})
			r.Rows++
		}
		if e == nil {
			e = rows.Err()
		}
		rows.Close()
		if e != nil {
			return out, e
		}
		if len(keys) > 0 {
			r.IdentitySHA256 = hex.EncodeToString(h.Sum(nil))
		}
		if rowID != "" {
			r.RowIDMappingSHA256 = hex.EncodeToString(mapping.Sum(nil))
		}
		out = append(out, r)
	}
	if err = tx.Commit(); err != nil {
		return out, err
	}
	return out, nil
}

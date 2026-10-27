package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// RawAuditCopyOptions controls one batch on a newly-created private working copy.
// No daemon endpoint calls this interface. It does not authorize a live DB writer.
type RawAuditCopyOptions struct {
	BatchRows   int
	MaxRowBytes int64
	BatchBytes  int64
	Budget      time.Duration
	Apply       bool
}
type RawAuditCopyResult struct {
	Scanned           int64 `json:"scanned"`
	Eligible          int64 `json:"eligible"`
	Migrated          int64 `json:"migrated"`
	Retained          int64 `json:"retained"`
	LogicalBytesDelta int64 `json:"logical_bytes_delta"`
	// Only a private copy runner persists raw cursor identities.
	NextCursor string `json:"-"`
}
type rawAuditCopyPlan struct {
	row         maintenanceRow
	side        maintenanceAudit
	replacement string
}

// FoldRawAuditCopyBatch only folds losslessly provable provider_event duplicates.
// It runs no DDL, checkpoint, VACUUM or general-purpose data deletion.
func FoldRawAuditCopyBatch(ctx context.Context, db *sql.DB, cursor string, o RawAuditCopyOptions) (result RawAuditCopyResult, err error) {
	result.NextCursor = cursor
	if o.BatchRows <= 0 || o.BatchBytes <= 0 || o.MaxRowBytes <= 0 || o.MaxRowBytes > o.BatchBytes || o.Budget <= 0 {
		return result, errors.New("invalid_copy_budget")
	}
	ctx, cancel := context.WithTimeout(ctx, o.Budget)
	defer cancel()
	read, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer read.Rollback()
	if err = validateHistoryRetentionSchema(ctx, read); err != nil {
		return result, err
	}
	rows, err := read.QueryContext(ctx, `SELECT r.object_id,r.conversation_id,r.semantic_sha256,r.prompt_sha256,r.original_bytes,r.original_sha256,r.expired_at,r.cutoff_at,r.fields_json,e.id,COALESCE(octet_length(e.payload_json),0) FROM ao_history_retention r LEFT JOIN conversation_provider_events e ON e.id=CAST(r.object_id AS INTEGER) WHERE r.object_kind='provider_event' AND r.object_id>? ORDER BY r.object_id LIMIT ?`, cursor, o.BatchRows)
	if err != nil {
		return result, err
	}
	type candidate struct {
		side maintenanceAudit
		id   sql.NullInt64
		size int64
	}
	candidates := []candidate{}
	for rows.Next() {
		c := candidate{}
		c.side.kind = "provider_event"
		err = rows.Scan(&c.side.id, &c.side.conversation, &c.side.semantic, &c.side.prompt, &c.side.bytes, &c.side.originalSHA, &c.side.at, &c.side.cutoff, &c.side.fields, &c.id, &c.size)
		if err != nil {
			break
		}
		candidates = append(candidates, c)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return result, err
	}
	plans := []rawAuditCopyPlan{}
	used := int64(0)
	for _, c := range candidates {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		if c.size > o.MaxRowBytes || !c.id.Valid {
			result.Scanned++
			result.Retained++
			result.NextCursor = c.side.id
			continue
		}
		if used+c.size > o.BatchBytes {
			break
		}
		used += c.size
		row, e := maintenanceRead(ctx, read, "conversation_provider_events", c.id.Int64, "raw")
		if e != nil {
			var reason maintenanceRetained
			if errors.As(e, &reason) {
				result.Scanned++
				result.Retained++
				result.NextCursor = c.side.id
				continue
			}
			return result, e
		}
		result.Scanned++
		result.NextCursor = c.side.id
		replacement, valid := losslessRawAudit(ctx, row, c.side)
		if !valid {
			result.Retained++
			continue
		}
		result.Eligible++
		plans = append(plans, rawAuditCopyPlan{row, c.side, replacement})
	}
	if err = read.Rollback(); err != nil {
		return result, err
	}
	if !o.Apply || len(plans) == 0 {
		return result, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	// Candidate body + all audit facts are compared, not hashes/length alone.
	for _, p := range plans {
		a := p.side
		update, e := tx.ExecContext(ctx, `UPDATE conversation_provider_events SET payload_json=? WHERE id=? AND conversation_id=? AND payload_json IS ? AND EXISTS(SELECT 1 FROM ao_history_retention WHERE object_kind='provider_event' AND object_id=? AND conversation_id=? AND semantic_sha256=? AND prompt_sha256=? AND original_bytes=? AND original_sha256=? AND expired_at=? AND cutoff_at=? AND fields_json=?)`, p.replacement, p.row["id"], p.row.text("conversation_id"), p.row.text("payload_json"), a.id, a.conversation, a.semantic, a.prompt, a.bytes, a.originalSHA, a.at, a.cutoff, a.fields)
		if e != nil {
			return result, e
		}
		n, e := update.RowsAffected()
		if e != nil {
			return result, e
		}
		if n != 1 {
			return result, errors.New("copy_candidate_changed")
		}
		deleted, e := tx.ExecContext(ctx, `DELETE FROM ao_history_retention WHERE object_kind='provider_event' AND object_id=? AND conversation_id=? AND semantic_sha256=? AND prompt_sha256=? AND original_bytes=? AND original_sha256=? AND expired_at=? AND cutoff_at=? AND fields_json=?`, a.id, a.conversation, a.semantic, a.prompt, a.bytes, a.originalSHA, a.at, a.cutoff, a.fields)
		if e != nil {
			return result, e
		}
		n, e = deleted.RowsAffected()
		if e != nil {
			return result, e
		}
		if n != 1 {
			return result, errors.New("copy_audit_changed")
		}
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	result.Migrated = int64(len(plans))
	for _, p := range plans {
		result.LogicalBytesDelta += int64(len(p.replacement)-len(p.row.text("payload_json"))) - int64(len(p.side.kind)+len(p.side.id)+len(p.side.conversation)+len(p.side.semantic)+len(p.side.prompt)+len(p.side.originalSHA)+len(p.side.at)+len(p.side.cutoff)+len(p.side.fields)+8)
	}
	return result, nil
}

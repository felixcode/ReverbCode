package store

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"
)

// Prepared values never come from the client. All source reads, JSON decoding,
// transformations and fingerprint/canonical hashing finish before writeMu is
// requested. Only fixed metadata and exact source CAS run in the writer.
type maintenanceRawProof struct {
	native       string
	hasBody      bool
	hasRetention bool
 bodyOnlyMarkers bool
}
type maintenanceAudit struct {
	kind, id, conversation, semantic, prompt, originalSHA, at, cutoff, fields string
	bytes                                                                     int64
}
type maintenancePrepared struct {
	id            any
	row           maintenanceRow
	cas           maintenanceRow
	bodyColumns   []string
	builtin       bool
	skip          string
	proof         *maintenanceRawProof
	updates       map[string]any
	fields        []string
	audits        []maintenanceAudit
	inlineExpired bool
	noRawBody     bool
 bodyNoChange bool
	size          int64
}

func prepareMaintenanceRaw(ctx context.Context, row maintenanceRow) (*maintenanceRawProof, error) {
	value, err := maintenanceJSONContext(ctx, row.text("payload_json"))
	if err != nil {
		return nil, err
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, maintenanceRetained("raw_not_object")
	}
	proof := &maintenanceRawProof{bodyOnlyMarkers:true}
	_, proof.hasRetention = obj["_retention"]
	proof.native, _ = obj["providerTurnId"].(string)
	for _, key := range maintenanceBodyKeys {
		proof.hasBody = proof.hasBody || maintenanceTruthy(obj[key])
        if maintenanceTruthy(obj[key])&&!historyBodyValueMarker(obj[key]){proof.bodyOnlyMarkers=false}
	}
	return proof, nil
}
func maintenanceQuoted(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }
func maintenanceAllColumns(ctx context.Context, db maintenanceDB, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+maintenanceQuoted(table)+`)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var n, nn, pk int
		var name, typ string
		var def any
		if err = rows.Scan(&n, &name, &typ, &nn, &def, &pk); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}
func maintenanceSize(ctx context.Context, db maintenanceDB, stage maintenanceStage, key string, id any, columns []string) (int64, error) {
	expr := []string{}
	for _, name := range columns {
		expr = append(expr, `COALESCE(octet_length(`+maintenanceQuoted(name)+`),0)`)
	}
	var size int64
	err := db.QueryRowContext(ctx, `SELECT `+strings.Join(expr, "+")+` FROM `+maintenanceQuoted(stage.table)+` WHERE `+maintenanceQuoted(key)+`=?`, id).Scan(&size)
	return size, err
}
func maintenanceBuiltinOriginal(ctx context.Context, db maintenanceDB, stage maintenanceStage, key string, id any, columns []string) (maintenanceRow, error) {
	expr := []string{}
	for _, name := range columns {
		expr = append(expr, `CAST(`+maintenanceQuoted(name)+` AS BLOB) AS `+maintenanceQuoted(name))
	}
	rows, err := db.QueryContext(ctx, `SELECT `+strings.Join(expr, ",")+` FROM `+maintenanceQuoted(stage.table)+` WHERE `+maintenanceQuoted(key)+`=?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	values := make([]any, len(columns))
	ptrs := make([]any, len(columns))
	for i := range values {
		ptrs[i] = &values[i]
	}
	if err = rows.Scan(ptrs...); err != nil {
		return nil, err
	}
	row := maintenanceRow{}
	for i, name := range columns {
		row[name] = values[i]
	}
	return row, nil
}
func prepareMaintenanceRows(ctx context.Context, db maintenanceDB, stage maintenanceStage, stageName, key string, ids []any, o HistoryMaintenanceOptions, now, cutoff time.Time, rawNoBodyFastPath ...bool) ([]maintenancePrepared, error) {
	if stageName == "gc:raw" { return prepareHistoryGC(ctx, db, ids) }
 if stageName == "gc:activity" { return prepareHistoryGCActivities(ctx,db,ids) }
 if stageName == "gc:audit" { return prepareHistoryGCAudits(ctx,db,ids) }
	columns := stage.fields
	var err error
	if stage.builtin {
		columns, err = maintenanceAllColumns(ctx, db, stage.table)
		if err != nil {
			return nil, err
		}
		if len(columns) == 0 {
			return nil, errors.New("missing builtin schema")
		}
	}
	plans := []maintenancePrepared{}
	used := int64(0)
	for _, id := range ids {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		p := maintenancePrepared{id: id, builtin: stage.builtin, bodyColumns: columns}

		if !stage.builtin {
			marked, e := maintenanceExists(ctx, db, `SELECT 1 FROM ao_history_retention WHERE object_kind=? AND object_id=?`, stage.kind, maintenanceID(id))
			if e != nil {
				return nil, e
			}
			if marked {
				p.skip = "already_expired"
				plans = append(plans, p)
				continue
			}
		}
		p.size, err = maintenanceSize(ctx, db, stage, key, id, columns)
		if errors.Is(err, sql.ErrNoRows) {
			p.skip = "row_missing"
			plans = append(plans, p)
			continue
		}
		if err != nil {
			return nil, err
		}
		if p.size > o.MaxRowBytes {
			p.skip = "row_size_limit"
			plans = append(plans, p)
			continue
		}
		if used+p.size > o.BatchBytes {
			break
		}
		used += p.size
		if stage.builtin {
			p.cas, err = maintenanceBuiltinOriginal(ctx, db, stage, key, id, columns)
			if err != nil {
				return nil, err
			}
			if p.cas == nil {
				p.skip = "row_missing"
			}
			plans = append(plans, p)
			continue
		}
		p.row, err = maintenanceRead(ctx, db, stage.table, id, stageName)
		if err != nil {
			var reason maintenanceRetained
			if errors.As(err, &reason) {
				p.skip = string(reason)
				plans = append(plans, p)
				continue
			}
			return nil, err
		}
		if p.row == nil {
			p.skip = "row_missing"
			plans = append(plans, p)
			continue
		}
		p.cas = maintenanceRow{}
		for _, name := range columns {
			if _, ok := p.row[name].(string); !ok {
				p.skip = "unsupported_body_type"
				break
			}
			p.cas[name] = p.row[name]
		}
		if p.skip != "" {
			plans = append(plans, p)
			continue
		}
		if stageName == "raw" {
			p.proof, err = prepareMaintenanceRaw(ctx, p.row)
			// The first strict parse already proved whether a marker exists.
			// Do not decode every unmarked metadata event a second time.
			if err == nil && p.proof.hasRetention {
				_, _, p.inlineExpired = rawInlineAudit(ctx, p.row, true)
			}
		}
		if p.inlineExpired && err == nil {
			plans = append(plans, p)
			continue
		}
		// A well-formed raw object with no body has nothing to expire. Do not
		// repeat owner/turn eligibility and transformation for metadata events.
		// The writer still rechecks metadata + exact body CAS before advancing.
		if len(rawNoBodyFastPath) > 0 && rawNoBodyFastPath[0] && stageName == "raw" && err == nil && p.proof.bodyOnlyMarkers {
			p.noRawBody = true
			plans = append(plans, p)
			continue
		}
		if err==nil && historyBodyAlreadyEmpty(p.row,stageName){p.bodyNoChange=true;plans=append(plans,p);continue}
        if err==nil {if reason:=historyBodyCoverageReason(ctx,p.row,stageName);reason!=""{p.skip=reason;plans=append(plans,p);continue}}
		if err == nil {
			var eligible bool
			eligible, err = maintenanceEligibleWithProof(ctx, db, p.row, stageName, cutoff, p.proof)
			if !eligible && err == nil {
				p.skip = "protected_owner_or_turn"
			}
		}
		if err == nil && p.skip == "" {
			p.updates, p.fields, err = maintenanceTransform(ctx, p.row, stageName, now.Format(time.RFC3339Nano), cutoff.Format(time.RFC3339Nano))
		}
		if err == nil && p.skip == "" {
			p.audits, err = prepareMaintenanceAudits(ctx, p.row, stageName, p.fields, now.Format(time.RFC3339Nano), cutoff.Format(time.RFC3339Nano))
		}
		if err != nil {
			var reason maintenanceRetained
			if errors.As(err, &reason) {
				p.skip = string(reason)
			} else {
				return nil, err
			}
		}
		plans = append(plans, p)
	}
	return plans, ctx.Err()
}
func maintenanceCAS(ctx context.Context, tx *sql.Tx, stage maintenanceStage, key string, p maintenancePrepared) (bool, error) {
	where := []string{maintenanceQuoted(key) + "=?"}
	args := []any{p.id}
	for _, name := range p.bodyColumns {
		expr := maintenanceQuoted(name)
		if p.builtin {
			expr = "CAST(" + expr + " AS BLOB)"
			// Some drivers represent x'' as a typed nil []byte. Binding it as
			// a parameter would become NULL and falsely reject an unchanged
			// empty field. SQL's literal empty BLOB preserves the distinction.
			empty := false
			switch value := p.cas[name].(type) {
			case []byte:
				empty = len(value) == 0
			case string:
				empty = value == ""
			}
			if empty {
				where = append(where, expr+" IS x''")
				continue
			}
		}
		where = append(where, expr+" IS ?")
		args = append(args, p.cas[name])
	}
	return maintenanceExists(ctx, tx, `SELECT 1 FROM `+maintenanceQuoted(stage.table)+` WHERE `+strings.Join(where, " AND "), args...)
}
func maintenanceMetadataEqual(original, current maintenanceRow, bodyColumns []string) bool {
	if current == nil {
		return false
	}
	for name, value := range current {
		if !reflect.DeepEqual(original[name], value) {
			return false
		}
	}
	body := map[string]bool{}
	for _, name := range bodyColumns {
		body[name] = true
	}
	for name := range original {
		if !body[name] {
			if _, ok := current[name]; !ok {
				return false
			}
		}
	}
	return true
}

// advanced=false retains the changed object at this prefix for another reader
// preparation. It is never recorded as cleared or given a stale marker.
func applyMaintenancePrepared(ctx context.Context, tx *sql.Tx, stage maintenanceStage, stageName, key string, p maintenancePrepared, cutoff, now time.Time, result *HistoryMaintenanceResult) (advanced bool, err error) {
	return applyMaintenancePreparedWithEligibility(ctx, tx, tx, stage, stageName, key, p, cutoff, now, result)
}

func applyMaintenancePreparedWithEligibility(ctx context.Context, tx *sql.Tx, eligibility maintenanceDB, stage maintenanceStage, stageName, key string, p maintenancePrepared, cutoff, now time.Time, result *HistoryMaintenanceResult) (advanced bool, err error) {
	if stageName == "gc:raw" { return applyHistoryGC(ctx, tx, p, cutoff, result) }
 if stageName == "gc:activity" { return applyHistoryGCActivity(ctx,tx,p,cutoff,result) }
 if stageName == "gc:audit" { return applyHistoryGCAudit(ctx,tx,p,cutoff,result) }
	if p.skip != "" {
		if p.skip == "already_expired" {
			marked, e := maintenanceExists(ctx, tx, `SELECT 1 FROM ao_history_retention WHERE object_kind=? AND object_id=?`, stage.kind, maintenanceID(p.id))
			if e != nil {
				return false, e
			}
			if !marked {
				result.Retained["marker_changed"]++
				return false, nil
			}
		}
		if p.skip != "row_missing" {
			result.Retained[p.skip]++
		}
		return true, nil
	}
	// A body may have grown after reader preparation. Reject by optimized
	// record-length metadata before evaluating any large original-value CAS.
	currentSize, sizeErr := maintenanceSize(ctx, tx, stage, key, p.id, p.bodyColumns)
	if errors.Is(sizeErr, sql.ErrNoRows) || sizeErr == nil && currentSize != p.size {
		result.Retained["concurrent_body_or_owner_change"]++
		return false, nil
	}
	if sizeErr != nil {
		return false, sizeErr
	}
	if !stage.builtin {
		marked, e := maintenanceExists(ctx, tx, `SELECT 1 FROM ao_history_retention WHERE object_kind=? AND object_id=?`, stage.kind, maintenanceID(p.id))
		if e != nil {
			return false, e
		}
		if marked {
			result.Retained["already_expired"]++
			return true, nil
		}
		current, e := maintenanceMetadata(ctx, tx, stage.table, stageName, p.id)
		if e != nil {
			var reason maintenanceRetained
			if errors.As(e, &reason) {
				result.Retained[string(reason)]++
				return false, nil
			}
			return false, e
		}
		if !maintenanceMetadataEqual(p.row, current, p.bodyColumns) {
			result.Retained["concurrent_body_or_owner_change"]++
			return false, nil
		}
		matches, e := maintenanceCAS(ctx, tx, stage, key, p)
		if e != nil {
			return false, e
		}
		if !matches {
			result.Retained["concurrent_body_or_owner_change"]++
			return false, nil
		}
		if p.bodyNoChange{result.Retained["body_already_empty_or_marker"]++;return true,nil}
        if reason:=historyBodyCoverageReason(ctx,current,stageName);reason!=""{result.Retained[reason]++;return true,nil}
		if p.inlineExpired {
			result.Retained["already_expired"]++
			return true, nil
		}
		if p.noRawBody {
			result.Retained["raw_no_body"]++
			return true, nil
		}
		eligible, e := maintenanceEligibleWithProof(ctx, eligibility, current, stageName, cutoff, p.proof)
		if e != nil {
			var reason maintenanceRetained
			if errors.As(e, &reason) {
				result.Retained[string(reason)]++
				return false, nil
			}
			return false, e
		}
		if !eligible {
			result.Retained["concurrent_owner_change"]++
			return false, nil
		}
		if len(p.fields) > 0 {
			keys := []string{}
			for name := range p.updates {
				keys = append(keys, name)
			}
			sort.Strings(keys)
			assign := []string{}
			args := []any{}
			for _, name := range keys {
				assign = append(assign, maintenanceQuoted(name)+"=?")
				args = append(args, p.updates[name])
			}
			if stageName == "message" || stageName == "activity" || stageName == "outputs" {
				assign = append(assign, "revision=revision+1")
			}
			args = append(args, p.id)
			if _, e = tx.ExecContext(ctx, `UPDATE `+maintenanceQuoted(stage.table)+` SET `+strings.Join(assign, ",")+` WHERE id=?`, args...); e != nil {
				return false, e
			}
			result.Compacted++
			for name, value := range p.updates {
				result.LogicalBytes += int64(len(maintenanceValueText(p.row[name])) - len(maintenanceValueText(value)))
			}
		} else {
			result.Audited++
		}
		n, e := insertMaintenanceAudits(ctx, tx, p.audits)
		if e != nil {
			return false, e
		}
		result.Markers += n
		return true, nil
	}
	matches, e := maintenanceCAS(ctx, tx, stage, key, p)
	if e != nil {
		return false, e
	}
	if !matches {
		result.Retained["concurrent_body_or_owner_change"]++
		return false, nil
	}
	deleted, e := maintenanceBuiltin(ctx, tx, stage, key, p.id, cutoff, now)
	if e != nil {
		var reason maintenanceRetained
		if errors.As(e, &reason) {
			result.Retained[string(reason)]++
			return true, nil
		}
		return false, e
	}
	if deleted {
		result.Deleted++
	}
	return true, nil
}
func maintenanceValueText(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return maintenanceID(v)
}

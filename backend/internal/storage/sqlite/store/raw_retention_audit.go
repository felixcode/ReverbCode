package store

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
)

// rawFieldMapSHA is a different digest domain from _retention.original_sha256:
// the former hashes canonical {payload_json: original}; the latter original bytes.
func rawFieldMapSHA(payload string) (string, error) {
	encoded, err := maintenanceCanonical(map[string]string{"payload_json": payload})
	if err != nil {
		return "", err
	}
	return maintenanceHash(encoded), nil
}

func rawAuditBinding(row maintenanceRow, fieldMapSHA string) map[string]any {
	return map[string]any{"id": maintenanceID(row["id"]), "conversation": row.text("conversation_id"), "sha256": fieldMapSHA}
}

func rawSHA(value any) bool {
	s, ok := value.(string)
	if !ok || len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// A complete inline audit is only produced by AO maintenance. Normal provider
// archives use an explicit top-level field map (no provider-supplied _retention).
// Short late-event markers and unknown extensions are not proof for audit GC.
func rawInlineAudit(ctx context.Context, row maintenanceRow, requireBinding bool) (map[string]any, map[string]any, bool) {
	value, err := maintenanceJSONContext(ctx, row.text("payload_json"))
	if err != nil {
		return nil, nil, false
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, nil, false
	}
	audit, ok := obj["_retention"].(map[string]any)
	if !ok || audit["protocol"] != "ao-history-v1" {
		return obj, nil, false
	}
	allowed := map[string]bool{"protocol": true, "expired_at": true, "cutoff_at": true, "fields": true, "original_bytes": true, "original_sha256": true, "field_map_audit": true}
	for k := range audit {
		if !allowed[k] {
			return obj, audit, false
		}
	}
	at, aok := audit["expired_at"].(string)
	cutoff, cok := audit["cutoff_at"].(string)
	a, ae := maintenanceTime(at)
	c, ce := maintenanceTime(cutoff)
	if !aok || !cok || ae != nil || ce != nil || c.After(a) || !rawSHA(audit["original_sha256"]) {
		return obj, audit, false
	}
	n, ok := audit["original_bytes"].(json.Number)
	if !ok {
		return obj, audit, false
	}
	bytes, e := strconv.ParseInt(string(n), 10, 64)
	if e != nil || bytes <= 0 {
		return obj, audit, false
	}
	fields, ok := audit["fields"].([]any)
	if !ok || len(fields) == 0 {
		return obj, audit, false
	}
	keys := map[string]bool{}
	for _, k := range maintenanceBodyKeys {
		keys[k] = true
	}
	seen := map[string]bool{}
	for _, value := range fields {
		name, ok := value.(string)
		if !ok || !keys[name] || seen[name] {
			return obj, audit, false
		}
		seen[name] = true
		switch v := obj[name].(type) {
		case string:
			if v != "" {
				return obj, audit, false
			}
		case []any:
			if len(v) != 0 {
				return obj, audit, false
			}
		case map[string]any:
			if len(v) != 0 {
				return obj, audit, false
			}
		default:
			return obj, audit, false
		}
	}
	for _, name := range maintenanceBodyKeys {
		if maintenanceTruthy(obj[name]) {
			return obj, audit, false
		}
	}
	binding, exists := audit["field_map_audit"]
	if !exists {
		return obj, audit, !requireBinding
	}
	b, ok := binding.(map[string]any)
	if !ok || len(b) != 3 || b["id"] != maintenanceID(row["id"]) || b["conversation"] != row.text("conversation_id") || !rawSHA(b["sha256"]) {
		return obj, audit, false
	}
	return obj, audit, true
}

// losslessRawAudit folds one historical, duplicate sidecar into its own complete
// inline audit. A missing inline, different facts or unknown extension is retained.
func losslessRawAudit(ctx context.Context, row maintenanceRow, side maintenanceAudit) (string, bool) {
	obj, audit, ok := rawInlineAudit(ctx, row, false)
	if !ok || side.kind != "provider_event" || side.id != maintenanceID(row["id"]) || side.conversation != row.text("conversation_id") || side.semantic != "" || side.prompt != "" || !rawSHA(side.originalSHA) {
		return "", false
	}
	// IDs are SQLite INTEGER identities, never text aliases such as 01 or +1.
	id, e := strconv.ParseInt(side.id, 10, 64)
	if e != nil || strconv.FormatInt(id, 10) != side.id {
		return "", false
	}
	bytes, e := strconv.ParseInt(string(audit["original_bytes"].(json.Number)), 10, 64)
	fields, e2 := maintenanceCanonical(audit["fields"])
	if e != nil || e2 != nil || bytes != side.bytes || audit["expired_at"] != side.at || audit["cutoff_at"] != side.cutoff || fields != side.fields {
		return "", false
	}
	if existing, yes := audit["field_map_audit"]; yes {
		b, ok := existing.(map[string]any)
		if !ok || b["sha256"] != side.originalSHA {
			return "", false
		}
	} else {
		audit["field_map_audit"] = rawAuditBinding(row, side.originalSHA)
	}
	encoded, err := maintenanceCanonical(obj)
	return encoded, err == nil && !strings.Contains(encoded, "\x00")
}

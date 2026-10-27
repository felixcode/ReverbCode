package store

import (
	"context"
	"errors"
)

// HistoryMaintenancePrefixCoverage describes only the existing PK selector's
// supported domain, not eligibility, removable backlog or freshness. It is used
// by the isolated copy runner and adds no live route or deletion rule.
type HistoryMaintenancePrefixCoverage struct {
	KeyType         string `json:"key_type"`
	LowerBound      string `json:"lower_bound"`
	UnsupportedRows int64  `json:"unsupported_rows"`
}

func (s *Store) HistoryMaintenanceScanPrefix(ctx context.Context, stageName string) (out HistoryMaintenancePrefixCoverage, err error) {
	stage, ok := maintenanceStages[stageName]
	if !ok {
		return out, errors.New("unsupported maintenance stage")
	}
	key, numeric, err := maintenanceKey(ctx, s.readDB, stage)
	if err != nil {
		return out, err
	}
	column := maintenanceQuoted(key)
	predicate := column + " IS NULL OR typeof(" + column + ")<>'text' OR " + column + "=''"
	out.KeyType = "text"
	out.LowerBound = "empty_string"
	if numeric {
		out.KeyType = "integer"
		out.LowerBound = "zero"
		predicate = column + " IS NULL OR typeof(" + column + ")<>'integer' OR " + column + "<=0"
	}
	err = s.readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+maintenanceQuoted(stage.table)+` WHERE `+predicate).Scan(&out.UnsupportedRows)
	return out, err
}

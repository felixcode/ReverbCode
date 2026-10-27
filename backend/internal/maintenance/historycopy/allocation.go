//go:build linux

package historycopy

import (
	"context"
	"database/sql"
	"strings"
)

type ObjectAllocation struct {
	Name           string `json:"name"`
	Table          string `json:"table"`
	Type           string `json:"type"`
	Pages          int64  `json:"pages"`
	AllocatedBytes int64  `json:"allocated_bytes"`
	PayloadBytes   int64  `json:"payload_bytes"`
	UnusedBytes    int64  `json:"unused_bytes"`
}
type AllocationReport struct {
	Available     bool               `json:"available"`
	UnknownReason string             `json:"unknown_reason,omitempty"`
	Objects       []ObjectAllocation `json:"objects"`
}

// CensusAllocation walks dbstat once. Page allocation, in-page unused bytes,
// reusable freelist pages and OS file lengths are different reporting units.
func CensusAllocation(ctx context.Context, db *sql.DB) (out AllocationReport, err error) {
	out.Objects = []ObjectAllocation{}
	rows, err := db.QueryContext(ctx, `SELECT d.name,COALESCE(s.tbl_name,d.name),COALESCE(s.type,'internal'),d.pageno,d.pgsize,d.payload,d.unused FROM dbstat('main',1) d LEFT JOIN sqlite_schema s ON s.name=d.name ORDER BY d.name`)
	if err != nil {
		if strings.Contains(err.Error(), "no such table: dbstat") {
			out.UnknownReason = "dbstat_unavailable"
			return out, nil
		}
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var r ObjectAllocation
		if err = rows.Scan(&r.Name, &r.Table, &r.Type, &r.Pages, &r.AllocatedBytes, &r.PayloadBytes, &r.UnusedBytes); err != nil {
			return out, err
		}
		out.Objects = append(out.Objects, r)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	out.Available = true
	return out, nil
}

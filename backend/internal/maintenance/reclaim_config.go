package maintenance

import (
	"errors"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func configuredPageReclaimOptions(history store.HistoryMaintenanceOptions) (o store.PageReclaimOptions, err error) {
	o = store.DefaultPageReclaimOptions()
	for suffix, dst := range map[string]*int64{"MAX_PAGES": &o.MaxPages, "MIN_FREE_PAGES": &o.MinFreePages} {
		if raw := os.Getenv("ALBANIA_AO_HISTORY_RECLAIM_" + suffix); raw != "" {
			*dst, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || *dst <= 0 {
				return o, errors.New("invalid reclaim size")
			}
		}
	}
	if raw := os.Getenv("ALBANIA_AO_HISTORY_RECLAIM_TRANSACTION_SECONDS"); raw != "" {
		value, e := strconv.ParseFloat(raw, 64)
		if e != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > float64(math.MaxInt64)/float64(time.Second) {
			return o, errors.New("invalid reclaim duration")
		}
		o.WriteBudget = time.Duration(value * float64(time.Second))
	}
	o.ReadBudget = min(o.ReadBudget, history.ReadBudget)
	o.WriteBudget = min(o.WriteBudget, history.WriteBudget)
	return o, o.Validate()
}

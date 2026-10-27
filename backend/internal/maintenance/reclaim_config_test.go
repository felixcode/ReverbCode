package maintenance

import (
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/store"
)

func TestReclaimConfigTightensAndRejectsOnlyNewSettings(t *testing.T) {
	history := store.DefaultHistoryMaintenanceOptions()
	t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_MAX_PAGES", "7")
	t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_MIN_FREE_PAGES", "32")
	t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_TRANSACTION_SECONDS", "0.5")
	o, err := configuredPageReclaimOptions(history)
	if err != nil || o.MaxPages != 7 || o.MinFreePages != 32 || o.WriteBudget != 250*time.Millisecond {
		t.Fatal(o, err)
	}
	for _, raw := range []string{"NaN", "+Inf", "0", "-1", "1e100", "0.0000000000000000000001"} {
		t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_TRANSACTION_SECONDS", raw)
		if _, err = configuredPageReclaimOptions(history); err == nil {
			t.Fatal("invalid new reclaim duration accepted", raw)
		}
		if history.WriteBudget != 250*time.Millisecond {
			t.Fatal("history configuration changed")
		}
	}
	t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_TRANSACTION_SECONDS", "0.1")
	o, err = configuredPageReclaimOptions(history)
	if err != nil || o.WriteBudget != 100*time.Millisecond {
		t.Fatal(o, err)
	}
	for _, suffix := range []string{"MAX_PAGES", "MIN_FREE_PAGES"} {
		for _, raw := range []string{"0", "-1", "bad", "9223372036854775808"} {
			t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_"+suffix, raw)
			if _, err = configuredPageReclaimOptions(history); err == nil {
				t.Fatal("invalid size accepted", suffix, raw)
			}
		}
		t.Setenv("ALBANIA_AO_HISTORY_RECLAIM_"+suffix, "1")
	}
}

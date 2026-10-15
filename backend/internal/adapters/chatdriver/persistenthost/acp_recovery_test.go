package persistenthost

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestACPRecoveryEvidenceUsesCompleteBoundedSameHostJournal(t *testing.T) {
	for _, scenario := range []string{"valid", "host-changed", "native-mismatch", "over-budget", "partial", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			dataDir := t.TempDir()
			d := Descriptor{SessionID: "session", PID: os.Getpid(), StartedAt: time.Now().UTC(), Address: "127.0.0.1:1", Token: "synthetic-token"}
			if err := writeDescriptor(dataDir, d); err != nil {
				t.Fatal(err)
			}
			dir, _ := hostDir(dataDir, d.SessionID)
			path := filepath.Join(dir, "acp-prompt.journal")
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			native := "native"
			if scenario == "native-mismatch" {
				native = "other-native"
			}
			for i := 0; i < 100; i++ {
				if _, err := fmt.Fprintf(file, "{\"method\":\"session/update\",\"params\":{\"sessionId\":%q,\"_meta\":{\"ao.persistentEventId\":\"event-%d\"}}}\n", native, i); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "partial" {
				_, _ = file.WriteString("{")
			}
			_ = file.Close()
			if scenario == "host-changed" {
				changed := d
				changed.Token = "different-token"
				if err := writeDescriptor(dataDir, changed); err != nil {
					t.Fatal(err)
				}
			}
			budget := int64(1 << 20)
			if scenario == "over-budget" {
				budget = 1
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "cancelled" {
				cancel()
			}
			ids, err := readACPRecoveryEvidence(ctx, dataDir, d, "native", budget)
			if scenario != "valid" {
				if err == nil {
					t.Fatalf("unsafe evidence accepted: %d", len(ids))
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(ids) != 100 || ids[99] != "event-99" {
				t.Fatalf("evidence incomplete: %d", len(ids))
			}
			info, _ := os.Stat(path)
			if info.Size() == 0 {
				t.Fatal("journal modified")
			}
		})
	}
}

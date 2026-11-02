package acp

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/persistenthost"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"sync/atomic"
	"testing"
	"time"
)

func TestACPDisconnectEvidenceAndDetachThenTerminate(t *testing.T) {
	conv, host := reconnectReplayFixture(t)
	var terminations atomic.Int32
	conv.proc.disconnectState = func() persistenthost.DisconnectState { return persistenthost.DisconnectPreserved }
	conv.proc.terminate = func() error { terminations.Add(1); return nil }
	if conv.DisconnectedProviderState() != ports.ChatDisconnectNotPersistent {
		t.Fatal("live connection classified disconnected")
	}
	if err := conv.ActivateLiveReconnect(context.Background(), "durable-turn"); err != nil {
		t.Fatal(err)
	}
	conv.Events()
	_ = host.Close()
	select {
	case <-conv.conn.Done():
	case <-time.After(time.Second):
		t.Fatal("connection did not close")
	}
	if conv.DisconnectedProviderState() != ports.ChatDisconnectPreserved {
		t.Fatal("adapter lost host proof")
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := conv.Terminate(); err != nil {
		t.Fatal(err)
	}
	if err := conv.Terminate(); err != nil {
		t.Fatal(err)
	}
	if terminations.Load() != 1 || conv.DisconnectedProviderState() != ports.ChatDisconnectEnded {
		t.Fatal("detach once swallowed explicit termination")
	}
}

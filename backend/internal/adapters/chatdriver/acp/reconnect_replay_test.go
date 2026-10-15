package acp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/adapters/chatdriver/persistenthost"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	acpsdk "github.com/coder/acp-go-sdk"
)

func reconnectReplayFixture(t *testing.T) (*conversation, net.Conn) {
	t.Helper()
	init, _ := json.Marshal(acpsdk.InitializeResponse{ProtocolVersion: acpsdk.ProtocolVersionNumber})
	session, _ := json.Marshal(acpsdk.NewSessionResponse{SessionId: "provider-session"})
	daemon, host := net.Pipe()
	t.Cleanup(func() { _ = host.Close() })
	driver := New(Config{Harness: domain.HarnessClaudeCode, Capabilities: ports.ChatCapabilities{ports.ChatCapabilityStreaming: true}, Launch: func(context.Context, LaunchConfig) (Launch, error) { return Launch{Command: "fake"}, nil }}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	driver.connectHost = func(context.Context, persistenthost.Config) (*persistenthost.Transport, error) {
		return &persistenthost.Transport{Stdin: daemon, Stdout: daemon, Reconnected: true, ACPState: &persistenthost.ACPState{InitializeResult: init, SessionResult: session, SessionID: "provider-session", ActivePrompt: true}}, nil
	}
	opened, err := driver.Resume(context.Background(), ports.ChatResumeConfig{SessionID: "ao-session", DataDir: t.TempDir(), WorkspacePath: t.TempDir(), ProviderConversationID: "provider-session"})
	if err != nil {
		t.Fatal(err)
	}
	conv := opened.(*conversation)
	t.Cleanup(func() { _ = conv.Close() })
	return conv, host
}

func TestReconnectReplayWaitsForConsumer(t *testing.T) {
	runReconnectReplay(t, 0)
}

func TestReconnectReplaySlowConsumer(t *testing.T) {
	runReconnectReplay(t, 100*time.Microsecond)
}

func runReconnectReplay(t *testing.T, consumerDelay time.Duration) {
	t.Helper()
	conv, host := reconnectReplayFixture(t)
	const frames = eventBuffer + 2048
	sent := make(chan error, 1)
	go func() {
		for i := 0; i < frames; i++ {
			_, err := fmt.Fprintf(host, "{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"_meta\":{\"ao.persistentEventId\":\"frame-%d\"},\"sessionId\":\"provider-session\",\"update\":{\"sessionUpdate\":\"agent_message_chunk\",\"content\":{\"type\":\"text\",\"text\":\"%d\"}}}}\n", i, i)
			if err != nil {
				sent <- err
				return
			}
		}
		sent <- nil
	}()
	if err := conv.ActivateLiveReconnect(context.Background(), "durable-turn"); err != nil {
		t.Fatal(err)
	}
	// Simulate persistence/admission work after activation but before projection.
	select {
	case <-conv.conn.Done():
		t.Fatal("replay overflow closed connection before event subscription")
	case err := <-sent:
		t.Fatalf("replay read before subscription: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	events := conv.Events()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for i := 0; i < frames; {
		select {
		case event := <-events:
			if event.Kind != ports.ChatEventMessageDelta {
				continue
			}
			if event.Delta != fmt.Sprint(i) || event.ProviderTurnID != "durable-turn" || event.ProviderEventID != fmt.Sprintf("frame-%d:0", i) {
				t.Fatalf("out of order frame %d: %+v", i, event)
			}
			if consumerDelay > 0 {
				time.Sleep(consumerDelay)
			}
			i++
		case <-conv.conn.Done():
			t.Fatal("connection closed while replaying")
		case <-deadline.C:
			t.Fatal("replay did not drain")
		}
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
}

func TestReconnectReplayUnsubscribedCloseReleasesReader(t *testing.T) {
	for _, activate := range []bool{false, true} {
		t.Run(fmt.Sprint(activate), func(t *testing.T) {
			conv, _ := reconnectReplayFixture(t)
			if activate {
				if err := conv.ActivateLiveReconnect(context.Background(), "durable-turn"); err != nil {
					t.Fatal(err)
				}
			}
			conv.discard()
			select {
			case <-conv.conn.Done():
			case <-time.After(time.Second):
				t.Fatal("discard left gated SDK reader alive")
			}
		})
	}
}

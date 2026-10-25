package sessionmanager

import (
	"context"
	"errors"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"testing"
)

func TestRetentionExpiredSavedPromptNeverRelaunches(t *testing.T) {
	_, _, _, err := freshLaunchArgv(context.Background(), nil, "session", "/tmp", domain.SessionMetadata{Prompt: "[AO history expired]", PromptExpired: true}, "", "", ports.AgentConfig{}, domain.KindWorker, "/tmp", false)
	if !errors.Is(err, ErrNotResumable) {
		t.Fatalf("expired saved prompt accepted: %v", err)
	}
	manager := &Manager{}
	_, _, _, err = manager.beginAsyncChatSpawn(context.Background(), asyncChatSpawn{record: domain.SessionRecord{Metadata: domain.SessionMetadata{Prompt: "[AO history expired]", PromptExpired: true}}})
	if !errors.Is(err, ErrNotResumable) {
		t.Fatalf("async path accepted expired saved prompt: %v", err)
	}
}

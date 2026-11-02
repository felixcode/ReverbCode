package sessionmanager

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"testing"
	"time"
)

func TestProviderProfilesSessionSeedAndResume(t *testing.T) {
	t.Setenv("CODEX_HOME", "/legacy")
	c := domain.ProjectConfig{ProviderProfiles: map[string]domain.ProviderProfile{"krill": {Label: "krill", Env: map[string]string{"CODEX_HOME": "/krill"}}}, ActiveProviderProfile: "krill"}
	rec := seedRecord(ports.SpawnConfig{Kind: domain.KindWorker}, c, time.Now())
	m := &Manager{}
	rec, env, err := m.pinProviderContext(context.Background(), rec, map[string]string{"CODEX_HOME": "/changed"})
	if err != nil || env["CODEX_HOME"] != "/krill" || env[domain.ProviderLabelEnv] != "krill" {
		t.Fatal("new worker profile missing")
	}
	c.ActiveProviderProfile = ""
	_, env, err = m.pinProviderContext(context.Background(), rec, c.Env)
	if err != nil || env["CODEX_HOME"] != "/krill" {
		t.Fatal("restore changed provider")
	}
	legacy := domain.SessionRecord{Kind: domain.KindWorker}
	_, env, err = m.pinProviderContext(context.Background(), legacy, c.Env)
	if err != nil || env["CODEX_HOME"] != "/legacy" {
		t.Fatal("legacy misclassified")
	}
	orch := seedRecord(ports.SpawnConfig{Kind: domain.KindOrchestrator}, c, time.Now())
	_, env, err = m.pinProviderContext(context.Background(), orch, map[string]string{"UNCHANGED": "yes"})
	if err != nil || env["UNCHANGED"] != "yes" || env[domain.ProviderLabelEnv] != "" {
		t.Fatal("orchestrator changed")
	}
}

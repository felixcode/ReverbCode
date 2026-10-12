package agent

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"testing"
)

func TestProviderProfilesModelDiscoveryRoleScope(t *testing.T) {
	t.Setenv("CODEX_HOME", "/inherited-original")
	projects := &fakeProjectLookup{records: map[string]domain.ProjectRecord{"demo": {Path: "/demo", Config: domain.ProjectConfig{Env: map[string]string{"CODEX_HOME": "/original"}, ActiveProviderProfile: "krill", ProviderProfiles: map[string]domain.ProviderProfile{"krill": {Env: map[string]string{"CODEX_HOME": "/krill"}, Label: "krill"}}}}}}
	s := &Service{projects: projects}
	worker, err := s.modelDiscoveryRequest(context.Background(), "codex", "demo", "/codex")
	if err != nil {
		t.Fatal(err)
	}
	orch, err := s.modelDiscoveryRequest(context.Background(), "codex", "@orchestrator:demo", "/codex")
	if err != nil {
		t.Fatal(err)
	}
	if worker.Env["CODEX_HOME"] != "/krill" || worker.Env[domain.ProviderLabelEnv] != "krill" || orch.Env["CODEX_HOME"] != "/original" || orch.Env[domain.ProviderLabelEnv] != "" {
		t.Fatal("catalog crossed provider role scope")
	}
	scope, err := s.modelCatalogScope(context.Background(), "@orchestrator:demo")
	if err != nil || scope != "@orchestrator:demo" {
		t.Fatalf("cache scope %q %v", scope, err)
	}
}

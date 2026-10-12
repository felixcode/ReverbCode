package modelcatalog

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"testing"
)

func TestProviderProfilesCodexCatalogLabelAndFingerprint(t *testing.T) {
	d := Discoverer{CodexModels: func(context.Context, ports.AgentModelDiscoveryRequest) ([]ports.ChatModel, error) {
		return []ports.ChatModel{{ID: "gpt-6.1-sol", DisplayName: "GPT-6.1-Sol", Default: true, Efforts: []string{"high"}}}, nil
	}}
	r := ports.AgentModelDiscoveryRequest{AgentID: "codex", Binary: "/bin/codex", Env: map[string]string{domain.ProviderLabelEnv: "krill"}}
	c, err := d.Discover(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	m := c.Models[0]
	if m.ID != "gpt-6.1-sol" || m.Label != "GPT-6.1-Sol · krill" || m.ProviderLabel != "krill" || m.DisplayName != m.Label || m.Efforts[0] != "high" {
		t.Fatalf("catalog %v", m)
	}
	one := d.CatalogFingerprint(context.Background(), r)
	r.Env[domain.ProviderLabelEnv] = "官方"
	if one == d.CatalogFingerprint(context.Background(), r) {
		t.Fatal("label change did not invalidate cached presentation")
	}
	if got := normalize(c.Models)[0]; got.ProviderLabel != "krill" {
		t.Fatal("normalization lost provider label")
	}
}

package codexappserver

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"testing"
)

func TestProviderProfilesAppServerModelListLabel(t *testing.T) {
	d, srv := newTestDriver(t)
	srv.reply("thread/start", `{"thread":{"id":"thread-1"},"model":"gpt-6.1-sol","reasoningEffort":"high","cwd":"/tmp/ws"}`)
	srv.reply("model/list", `{"data":[{"id":"gpt-6.1-sol","displayName":"GPT-6.1-Sol","isDefault":true,"hidden":false,"supportedReasoningEfforts":[{"reasoningEffort":"high"}]}]}`)
	c, err := d.Start(context.Background(), ports.ChatStartConfig{WorkspacePath: "/tmp/ws", Env: map[string]string{domain.ProviderLabelEnv: "krill"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	models, err := c.(ports.ChatModelLister).ListModels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0].ID != "gpt-6.1-sol" || models[0].DisplayName != "GPT-6.1-Sol · krill" || models[0].ProviderLabel != "krill" || models[0].DefaultEffort != "high" {
		t.Fatalf("models %v", models)
	}
}

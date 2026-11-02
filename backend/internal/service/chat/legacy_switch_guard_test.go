package chat

import (
	"context"
	"errors"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"testing"
)

type retainedLegacySwitchStore struct {
	Store
	blocked bool
}

func (s retainedLegacySwitchStore) LegacySwitchRecoveryRequired(context.Context, domain.SessionID) (bool, error) {
	return s.blocked, nil
}
func TestLegacySwitchRecoveryFencePrecedesNewProviderAdmission(t *testing.T) {
	for _, blocked := range []bool{true, false} {
		svc := &Service{store: retainedLegacySwitchStore{blocked: blocked}}
		release, err := svc.lockProviderSwitch(context.Background(), "synthetic-session")
		if !blocked {
			if err != nil {
				t.Fatal(err)
			}
			release()
			continue
		}
		var refusal *domain.ProviderSwitchError
		if !errors.As(err, &refusal) || refusal.Kind != domain.ProviderSwitchRefusalRecoveryRequired || release != nil {
			t.Fatal("retained recovery admitted")
		}
	}
}

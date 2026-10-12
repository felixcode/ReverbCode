package sessionmanager

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

type providerContextStore interface {
	EnsureProviderContext(context.Context, string, string, *domain.ProviderContext) (*domain.ProviderContext, error)
}

func (m *Manager) pinProviderContext(ctx context.Context, rec domain.SessionRecord, fallback map[string]string) (domain.SessionRecord, map[string]string, error) {
	if rec.Kind == domain.KindOrchestrator {
		return rec, fallback, nil
	}
	p := rec.Metadata.ProviderContext
	if p == nil {
		p = domain.LegacyProviderContext(fallback)
	}
	if store, ok := m.store.(providerContextStore); ok {
		var err error
		p, err = store.EnsureProviderContext(ctx, "session", string(rec.ID), p)
		if err != nil {
			return rec, nil, err
		}
	}
	rec.Metadata.ProviderContext = p
	rec.ProviderProfile = p.Profile
	rec.ProviderLabel = p.Label
	return rec, p.LaunchEnv(), nil
}

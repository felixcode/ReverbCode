package review

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

func (e *Engine) providerEnv(ctx context.Context, r domain.Review, projectID domain.ProjectID) (map[string]string, error) {
	p := r.ProviderContext
	if p == nil {
		env, err := e.projectEnv(ctx, projectID)
		if err != nil {
			return nil, err
		}
		p = domain.LegacyProviderContext(env)
	}
	if store, ok := e.store.(interface {
		EnsureProviderContext(context.Context, string, string, *domain.ProviderContext) (*domain.ProviderContext, error)
	}); ok {
		var err error
		p, err = store.EnsureProviderContext(ctx, "review", r.ID, p)
		if err != nil {
			return nil, err
		}
	}
	return p.LaunchEnv(), nil
}

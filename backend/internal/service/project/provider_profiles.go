package project

import (
	"context"
	"sort"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/apierr"
)

type ProviderProfileSummary struct {
	Name    string   `json:"name"`
	Label   string   `json:"label"`
	Roles   []string `json:"roles"`
	EnvKeys []string `json:"envKeys"`
}
type ProviderProfilesResult struct {
	ActiveProviderProfile string                   `json:"activeProviderProfile"`
	Profiles              []ProviderProfileSummary `json:"profiles"`
}
type ProviderProfilesPatch struct {
	ProviderProfiles      *map[string]domain.ProviderProfile `json:"providerProfiles,omitempty"`
	ActiveProviderProfile *string                            `json:"activeProviderProfile,omitempty"`
}
type ProviderProfilesManager interface {
	ProviderProfiles(context.Context, domain.ProjectID) (ProviderProfilesResult, error)
	PatchProviderProfiles(context.Context, domain.ProjectID, ProviderProfilesPatch) (ProviderProfilesResult, error)
}

func (m *Service) ProviderProfiles(ctx context.Context, id domain.ProjectID) (ProviderProfilesResult, error) {
	row, ok, err := m.store.GetProject(ctx, string(id))
	if err != nil {
		return ProviderProfilesResult{}, err
	}
	if !ok || !row.ArchivedAt.IsZero() {
		return ProviderProfilesResult{}, apierr.NotFound("PROJECT_NOT_FOUND", "Unknown project")
	}
	out := ProviderProfilesResult{ActiveProviderProfile: row.Config.ActiveProviderProfile, Profiles: []ProviderProfileSummary{}}
	for name, p := range row.Config.ProviderProfiles {
		keys := []string{}
		for key := range p.Env {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		roles := append([]string(nil), p.Roles...)
		if len(roles) == 0 {
			roles = []string{"worker", "reviewer"}
		}
		out.Profiles = append(out.Profiles, ProviderProfileSummary{Name: name, Label: p.Label, Roles: roles, EnvKeys: keys})
	}
	sort.Slice(out.Profiles, func(i, j int) bool { return out.Profiles[i].Name < out.Profiles[j].Name })
	return out, nil
}
func (m *Service) PatchProviderProfiles(ctx context.Context, id domain.ProjectID, in ProviderProfilesPatch) (ProviderProfilesResult, error) {
	if err := validateProjectID(id); err != nil {
		return ProviderProfilesResult{}, err
	}
	if in.ProviderProfiles == nil && in.ActiveProviderProfile == nil {
		return ProviderProfilesResult{}, apierr.Invalid("INVALID_PROVIDER_PROFILE", "Empty provider patch", nil)
	}
	store, ok := m.store.(interface {
		PatchProviderProfiles(context.Context, string, *map[string]domain.ProviderProfile, *string) error
	})
	if !ok {
		return ProviderProfilesResult{}, apierr.Internal("PROVIDER_PROFILES_UNAVAILABLE", "Provider profiles unavailable")
	}
	if err := store.PatchProviderProfiles(ctx, string(id), in.ProviderProfiles, in.ActiveProviderProfile); err != nil {
		return ProviderProfilesResult{}, apierr.Invalid("INVALID_PROVIDER_PROFILE", "Unknown project or invalid provider profile", nil)
	}
	m.modelScopeChanged(string(id))
	return m.ProviderProfiles(ctx, id)
}

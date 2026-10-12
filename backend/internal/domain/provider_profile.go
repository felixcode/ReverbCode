package domain

import (
	"fmt"
	"os"
	"strings"
)

const ProviderLabelEnv = "AO_PROVIDER_LABEL"

type ProviderProfile struct {
	Env   map[string]string `json:"env,omitempty"`
	Label string            `json:"label,omitempty"`
	Roles []string          `json:"roles,omitempty"`
}

// ProviderContext is immutable launch input, never a credential-bearing API DTO.
type ProviderContext struct {
	Profile string            `json:"profile,omitempty"`
	Label   string            `json:"label,omitempty"`
	Env     map[string]string `json:"env"`
}

func (c ProjectConfig) ValidateProviderProfiles() error {
	if c.ActiveProviderProfile != "" {
		if _, ok := c.ProviderProfiles[c.ActiveProviderProfile]; !ok {
			return fmt.Errorf("activeProviderProfile: unknown profile")
		}
	}
	for name, p := range c.ProviderProfiles {
		if strings.TrimSpace(name) != name || name == "" {
			return fmt.Errorf("providerProfiles: invalid name")
		}
		if err := validateNameComponent("provider profile", name); err != nil {
			return err
		}
		if len(p.Label) > 128 || strings.ContainsAny(p.Label, "\x00\r\n") {
			return fmt.Errorf("providerProfiles.%s: invalid label", name)
		}
		// Reuse the reserved-namespace and env-name validation without recursion.
		if err := (ProjectConfig{Env: p.Env}).Validate(); err != nil {
			return fmt.Errorf("providerProfiles.%s: %w", name, err)
		}
		seen := map[string]bool{}
		for _, role := range p.Roles {
			if (role != "worker" && role != "reviewer") || seen[role] {
				return fmt.Errorf("providerProfiles.%s: invalid/duplicate role", name)
			}
			seen[role] = true
		}
	}
	return nil
}

// LegacyProviderContext pins only explicit project env and inherited provider
// settings. AO identity/capability variables are freshly generated at launch.
func LegacyProviderContext(env map[string]string) *ProviderContext {
	out := &ProviderContext{Env: map[string]string{}}
	for k, v := range env {
		out.Env[k] = v
	}
	for _, k := range []string{"CODEX_HOME", "CODEX_AUTHAPI_BASE_URL"} {
		if _, ok := out.Env[k]; !ok {
			if value, exists := os.LookupEnv(k); exists {
				out.Env[k] = value
			}
		}
	}
	return out
}

func (c ProjectConfig) ResolveProvider(role string) *ProviderContext {
	out := LegacyProviderContext(c.Env)
	if role != "worker" && role != "reviewer" {
		return out
	}
	p, ok := c.ProviderProfiles[c.ActiveProviderProfile]
	if !ok {
		return out
	}
	applies := len(p.Roles) == 0
	for _, r := range p.Roles {
		applies = applies || r == role
	}
	if !applies {
		return out
	}
	out.Profile = c.ActiveProviderProfile
	out.Label = p.Label
	for k, v := range p.Env {
		out.Env[k] = v
	}
	return out
}

func (p *ProviderContext) LaunchEnv() map[string]string {
	out := make(map[string]string, len(p.Env)+1)
	for k, v := range p.Env {
		out[k] = v
	}
	if p.Label != "" {
		out[ProviderLabelEnv] = p.Label
	}
	return out
}

func SessionProviderEnv(rec SessionRecord, fallback map[string]string) map[string]string {
	if rec.Kind == KindOrchestrator {
		return fallback
	}
	if rec.Metadata.ProviderContext != nil {
		return rec.Metadata.ProviderContext.LaunchEnv()
	}
	return LegacyProviderContext(fallback).LaunchEnv()
}

func ProviderDisplayName(name, label string) string {
	if label == "" {
		return name
	}
	suffix := " · " + label
	if strings.HasSuffix(name, suffix) {
		return name
	}
	return name + suffix
}

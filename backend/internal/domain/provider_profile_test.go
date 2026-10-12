package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestProviderProfilesRolesAndPinning(t *testing.T) {
	t.Setenv("CODEX_HOME", "/original")
	var c ProjectConfig
	if err := json.Unmarshal([]byte(`{"env":{"BASE":"preserved"},"activeProviderProfile":"krill","providerProfiles":{"krill":{"env":{"CODEX_HOME":"/krill"},"label":"krill"},"official":{"env":{"CODEX_HOME":"/official"},"label":"官方","roles":["worker"]}}}`), &c); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	worker := c.ResolveProvider("worker")
	reviewer := c.ResolveProvider("reviewer")
	if worker.Profile != "krill" || worker.Env["BASE"] != "preserved" || reviewer.Env["CODEX_HOME"] != "/krill" {
		t.Fatal("default role resolution")
	}
	if p := c.ResolveProvider("orchestrator"); p.Profile != "" || p.Env["CODEX_HOME"] != "/original" {
		t.Fatal("orchestrator was changed")
	}
	if c.ResolveProvider("unknown-role").Profile != "" {
		t.Fatal("unknown role received profile")
	}
	c.ActiveProviderProfile = "official"
	if worker.LaunchEnv()["CODEX_HOME"] != "/krill" {
		t.Fatal("old env changed")
	}
	if c.ResolveProvider("reviewer").Profile != "" || c.ResolveProvider("worker").Profile != "official" {
		t.Fatal("role scope ignored")
	}
	rec := SessionRecord{Kind: KindWorker, Metadata: SessionMetadata{ProviderContext: worker}}
	if SessionProviderEnv(rec, c.Env)["CODEX_HOME"] != "/krill" {
		t.Fatal("resume selected active profile")
	}
	data, _ := json.Marshal(rec)
	if strings.Contains(string(data), "/krill") {
		t.Fatal("private env leaked")
	}
}

func TestProviderProfilesRejectInvalidConfig(t *testing.T) {
	for name, c := range map[string]ProjectConfig{
		"unknown":        {ActiveProviderProfile: "missing"},
		"reserved":       {ProviderProfiles: map[string]ProviderProfile{"p": {Env: map[string]string{"AO_SESSION_ID": "other"}}}},
		"role":           {ProviderProfiles: map[string]ProviderProfile{"p": {Roles: []string{"orchestrator"}}}},
		"duplicate-role": {ProviderProfiles: map[string]ProviderProfile{"p": {Roles: []string{"worker", "worker"}}}},
		"bad-label":      {ProviderProfiles: map[string]ProviderProfile{"p": {Label: "bad\nlabel"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if c.Validate() == nil {
				t.Fatal("accepted invalid profile")
			}
		})
	}
}

func TestProviderProfilesLegacyAndLabel(t *testing.T) {
	t.Setenv("CODEX_HOME", "/legacy")
	c := ProjectConfig{ActiveProviderProfile: "p", ProviderProfiles: map[string]ProviderProfile{"p": {Env: map[string]string{"CODEX_HOME": "/new"}}}}
	rec := SessionRecord{Kind: KindWorker}
	if SessionProviderEnv(rec, c.Env)["CODEX_HOME"] != "/legacy" {
		t.Fatal("legacy incorrectly selected active profile")
	}
	if ProviderDisplayName("GPT", "krill") != "GPT · krill" || ProviderDisplayName("GPT · krill", "krill") != "GPT · krill" || ProviderDisplayName("GPT", "") != "GPT" {
		t.Fatal("label presentation")
	}
}

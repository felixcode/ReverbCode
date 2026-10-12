package store_test

import (
	"context"
	"encoding/json"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestProviderProfilesAtomicPatchPreservesOtherConfig(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	p, _, _ := s.GetProject(ctx, "mer")
	p.Config = domain.ProjectConfig{DefaultBranch: "develop", AgentRules: "rules", Env: map[string]string{"BASE": "original"}, Worker: domain.RoleOverride{Harness: domain.HarnessCodex, AgentConfig: domain.AgentConfig{Model: "gpt-6.1-sol", Effort: "high"}}}
	if err := s.UpsertProject(ctx, p); err != nil {
		t.Fatal(err)
	}
	profiles := map[string]domain.ProviderProfile{"a": {Env: map[string]string{"CODEX_HOME": "/a"}, Label: "a"}, "b": {Env: map[string]string{"CODEX_HOME": "/b"}, Label: "b"}}
	if err := s.PatchProviderProfiles(ctx, "mer", &profiles, nil); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "a"
			if i%2 == 0 {
				name = "b"
			}
			errs <- s.PatchProviderProfiles(ctx, "mer", nil, &name)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	p, _, _ = s.GetProject(ctx, "mer")
	if p.Config.DefaultBranch != "develop" || p.Config.Env["BASE"] != "original" || p.Config.AgentRules != "rules" || p.Config.Worker.AgentConfig.Model != "gpt-6.1-sol" || p.Config.Worker.AgentConfig.Effort != "high" || len(p.Config.ProviderProfiles) != 2 {
		t.Fatal("unrelated config overwritten")
	}
	before, _ := json.Marshal(p.Config)
	bad := "missing"
	if s.PatchProviderProfiles(ctx, "mer", nil, &bad) == nil {
		t.Fatal("unknown accepted")
	}
	p, _, _ = s.GetProject(ctx, "mer")
	after, _ := json.Marshal(p.Config)
	if string(before) != string(after) {
		t.Fatal("invalid request changed config")
	}
}

func TestProviderProfilesPersistSessionAndReviewWithoutEnvLeak(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	old := &domain.ProviderContext{Profile: "a", Label: "krill", Env: map[string]string{"CODEX_HOME": "/original-thread-home", "SYNTHETIC_SECRET": "synthetic-only"}}
	rec := sampleRecord("mer")
	rec.Metadata.ProviderContext = old
	rec, err := s.CreateSession(ctx, rec)
	if err != nil {
		t.Fatal(err)
	}
	new := &domain.ProviderContext{Profile: "b", Label: "官方", Env: map[string]string{"CODEX_HOME": "/other"}}
	p, err := s.EnsureProviderContext(ctx, "session", string(rec.ID), new)
	if err != nil || p.Profile != "a" {
		t.Fatalf("session pin %v %v", p, err)
	}
	// Independent load models daemon restart and metadata/controller replacement.
	loaded, ok, err := s.GetSession(ctx, rec.ID)
	if err != nil || !ok || domain.SessionProviderEnv(loaded, new.Env)["CODEX_HOME"] != "/original-thread-home" || loaded.ProviderLabel != "krill" {
		t.Fatal("session resume lost pin")
	}
	data, _ := json.Marshal(loaded)
	if strings.Contains(string(data), "synthetic-only") || strings.Contains(string(data), "/original-thread-home") {
		t.Fatal("API leaked env")
	}
	rows, err := s.ListAllSessions(ctx)
	if err != nil || rows[0].ProviderProfile != "a" {
		t.Fatal("list missing profile")
	}
	now := time.Now().UTC()
	review := domain.Review{ID: "provider-review", SessionID: rec.ID, ProjectID: rec.ProjectID, Harness: domain.ReviewerCodex, CreatedAt: now, UpdatedAt: now, ProviderContext: old}
	if err = s.UpsertReview(ctx, review); err != nil {
		t.Fatal(err)
	}
	review.ProviderContext = new
	if err = s.UpsertReview(ctx, review); err != nil {
		t.Fatal(err)
	}
	r, ok, err := s.GetReviewByID(ctx, review.ID)
	if err != nil || !ok || r.ProviderContext.Env["CODEX_HOME"] != "/original-thread-home" {
		t.Fatal("review restore lost pin")
	}
	data, _ = json.Marshal(r)
	if strings.Contains(string(data), "synthetic-only") {
		t.Fatal("review API leaked env")
	}
	if _, err = s.EnsureProviderContext(ctx, "session", "absent", old); err == nil {
		t.Fatal("missing owner accepted")
	}
}

func TestProviderProfilesLegacyFirstWriterWins(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	seedProject(t, s, "mer")
	rec, err := s.CreateSession(ctx, sampleRecord("mer"))
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := s.GetSession(ctx, rec.ID)
	if err != nil || loaded.Metadata.ProviderContext != nil {
		t.Fatal("migration fabricated historical profile")
	}
	t.Setenv("CODEX_HOME", "/legacy")
	legacy := domain.LegacyProviderContext(nil)
	first, err := s.EnsureProviderContext(ctx, "session", string(rec.ID), legacy)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.EnsureProviderContext(ctx, "session", string(rec.ID), &domain.ProviderContext{Profile: "krill", Env: map[string]string{"CODEX_HOME": "/krill"}})
	if err != nil || first.Profile != "" || second.Profile != "" || second.Env["CODEX_HOME"] != "/legacy" {
		t.Fatal("legacy switched")
	}
}

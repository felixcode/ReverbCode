package review

import (
	"context"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"testing"
	"time"
)

func TestProviderProfilesReviewerNewAndRestore(t *testing.T) {
	t.Setenv("CODEX_HOME", "/legacy")
	c := domain.ProjectConfig{ProviderProfiles: map[string]domain.ProviderProfile{"krill": {Label: "krill", Env: map[string]string{"CODEX_HOME": "/krill"}}}, ActiveProviderProfile: "krill"}
	e := New(Deps{Store: &fakeStore{}, Projects: fakeProjects{cfg: c}})
	w := domain.SessionRecord{ID: "mer-1", ProjectID: "mer"}
	r, err := e.upsertReview(context.Background(), w, domain.ReviewerCodex, "", "", "", domain.ActivityIdle, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	env, err := e.providerEnv(context.Background(), r, w.ProjectID)
	if err != nil || env["CODEX_HOME"] != "/krill" {
		t.Fatal("new reviewer not using profile")
	}
	c.ActiveProviderProfile = ""
	e.projects = fakeProjects{cfg: c}
	r, err = e.upsertReview(context.Background(), w, domain.ReviewerCodex, "", "", "", domain.ActivityIdle, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	env, err = e.providerEnv(context.Background(), r, w.ProjectID)
	if err != nil || env["CODEX_HOME"] != "/krill" {
		t.Fatal("reviewer reused thread changed env")
	}
	env, err = e.providerEnv(context.Background(), domain.Review{}, w.ProjectID)
	if err != nil || env["CODEX_HOME"] != "/legacy" {
		t.Fatal("legacy reviewer changed")
	}
}

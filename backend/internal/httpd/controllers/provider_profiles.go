package controllers

import (
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/envelope"
	projectsvc "github.com/aoagents/agent-orchestrator/backend/internal/service/project"
	"github.com/go-chi/chi/v5"
	"net/http"
)

func (c *ProjectsController) providerProfiles(w http.ResponseWriter, r *http.Request) {
	mgr, ok := c.Mgr.(projectsvc.ProviderProfilesManager)
	if !ok {
		envelope.WriteAPIError(w, r, http.StatusNotImplemented, "not_implemented", "PROVIDER_PROFILES_UNAVAILABLE", "Provider profiles unavailable", nil)
		return
	}
	id := domain.ProjectID(chi.URLParam(r, "id"))
	var out projectsvc.ProviderProfilesResult
	var err error
	if r.Method == http.MethodPatch {
		var in projectsvc.ProviderProfilesPatch
		if err = decodeJSONStrict(r, &in); err != nil {
			envelope.WriteAPIError(w, r, http.StatusBadRequest, "bad_request", "INVALID_JSON", "Invalid provider patch", nil)
			return
		}
		out, err = mgr.PatchProviderProfiles(r.Context(), id, in)
	} else {
		out, err = mgr.ProviderProfiles(r.Context(), id)
	}
	if err != nil {
		envelope.WriteError(w, r, err)
		return
	}
	envelope.WriteJSON(w, http.StatusOK, out)
}

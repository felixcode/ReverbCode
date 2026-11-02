package controllers

import (
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRetentionExpiredHistoryGetsExplicitConflict(t *testing.T) {
	for _, write := range []func(http.ResponseWriter, *http.Request, error){writeConversationRetryError, writeConversationEditError, writeConversationError} {
		r := httptest.NewRecorder()
		write(r, httptest.NewRequest(http.MethodPost, "/", nil), domain.ErrHistoryContentExpired)
		if r.Code != http.StatusConflict || !strings.Contains(r.Body.String(), "CHAT_HISTORY_CONTENT_EXPIRED") {
			t.Fatalf("expiry was not explicit conflict: %d", r.Code)
		}
	}
}

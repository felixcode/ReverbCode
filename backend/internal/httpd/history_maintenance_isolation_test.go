package httpd

import (
	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHistoryMaintenanceIsAbsentFromPublicAndMobileRouters(t *testing.T) {
	router := newTestRouter(config.Config{}, discardLogger(), nil)
	for name, handler := range map[string]http.Handler{"primary": router, "mobile": lanControlBlock(router)} {
		for _, path := range []string{"/v1/history/batch", "/api/v1/history/batch", "/api/v1/dev/history/batch"} {
			record := httptest.NewRecorder()
			request := httptest.NewRequest("POST", path, strings.NewReader(`{"stage":"raw"}`))
			handler.ServeHTTP(record, request)
			if record.Code >= 200 && record.Code < 300 {
				t.Fatal("network maintenance route exposed", name, path, record.Code)
			}
		}
	}
}

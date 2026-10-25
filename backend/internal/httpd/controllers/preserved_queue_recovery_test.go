package controllers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aoagents/agent-orchestrator/backend/internal/config"
	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd"
	"github.com/aoagents/agent-orchestrator/backend/internal/httpd/requestscope"
)

type queueRecoveryHTTPService struct {
	*fakeConversationService
	calls int
}

func (s *queueRecoveryHTTPService) RecoverPreservedQueue(_ context.Context, session domain.SessionID, req domain.PreservedQueueRecoveryRequest) (domain.PreservedQueueRecoveryResult, error) {
	s.calls++
	hash, err := domain.PreservedQueueRecoveryDigest(session, req)
	return domain.PreservedQueueRecoveryResult{Restored: len(req.Turns), RequestSHA256: hash, Dispatch: "held_busy"}, err
}
func queueHTTPProof() domain.PreservedQueueRecoveryRequest {
	h := strings.Repeat("a", 64)
	return domain.PreservedQueueRecoveryRequest{Protocol: 1, ConversationID: "conv", BranchID: "branch", NativeID: "native", BoundTurnID: "bound", BoundProviderTurnID: "provider", FailureAt: "old", BeforeSnapshotSHA256: h, Turns: []domain.PreservedQueueTurnProof{{ID: "turn", BeforeState: "queued", PreviousGeneration: "old", RequestedAt: "then", Inputs: []domain.PreservedQueueInputProof{{ID: "message", TextSHA256: h, DeliverySHA256: h}}}}}
}
func TestPreservedQueueRecoveryLocalListenerAndStrictBody(t *testing.T) {
	for _, scenario := range []string{"local", "lan-with-forged-host", "forwarded", "origin", "foreign-host", "unknown-field"} {
		t.Run(scenario, func(t *testing.T) {
			svc := &queueRecoveryHTTPService{fakeConversationService: &fakeConversationService{}}
			router := httpd.NewRouterWithControl(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, httpd.APIDeps{Sessions: newFakeSessionService(), Conversations: svc}, httpd.ControlDeps{})
			data, _ := json.Marshal(queueHTTPProof())
			if scenario == "unknown-field" {
				data = append(data[:len(data)-1], []byte(`,"notAProof":true}`)...)
			}
			req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/v1/dev/sessions/session/preserved-queue/recover", bytes.NewReader(data))
			req.RemoteAddr = "127.0.0.1:12345"
			switch scenario {
			case "lan-with-forged-host":
				req = req.WithContext(requestscope.WithLAN(req.Context()))
			case "forwarded":
				req.Header.Set("X-Forwarded-For", "198.51.100.1")
			case "origin":
				req.Header.Set("Origin", "http://remote.example")
			case "foreign-host":
				req.Host = "remote.example"
			}
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			if scenario == "local" {
				if recorder.Code != 200 || svc.calls != 1 {
					t.Fatalf("local status=%d", recorder.Code)
				}
			} else if recorder.Code < 400 || svc.calls != 0 {
				t.Fatalf("remote/malformed accepted: %d", recorder.Code)
			}
		})
	}
}

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
	"github.com/aoagents/agent-orchestrator/backend/internal/runfile"
)

func TestInvisiblePIDHandshake(t *testing.T) {
	for _, name := range []string{"matching", "wrong-service", "wrong-pid", "old-start", "old-run", "missing-start", "missing-run", "not-ok", "legacy", "http-error", "redirect", "changed-after", "changed-before", "removed", "corrupt", "network-denied", "timeout", "missing-record-start", "missing-record-run", "connection-refused", "post-redirect"} {
		t.Run(name, func(t *testing.T) {
			cfg := setConfigEnv(t)
			info := runfile.Info{PID: 4242, StartedAt: time.Unix(100, 42).UTC(), AppRunID: "app-launch-1"}
			posts, probes, redirects := 0, 0, 0
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirects++; w.WriteHeader(200) }))
			defer target.Close()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/healthz" {
					posts++
					if name == "post-redirect" {
						http.Redirect(w, r, target.URL, http.StatusFound)
						return
					}
					w.WriteHeader(http.StatusNoContent)
					return
				}
				probes++
				p := probeResult{Status: "ok", Service: daemonmeta.ServiceName, PID: 4242, StartedAt: time.Unix(100, 42).UTC(), AppRunID: "app-launch-1"}
				switch name {
				case "wrong-service":
					p.Service = "other"
				case "wrong-pid":
					p.PID++
				case "old-start":
					p.StartedAt = p.StartedAt.Add(time.Second)
				case "old-run":
					p.AppRunID = "app-launch-2"
				case "missing-start":
					p.StartedAt = time.Time{}
				case "missing-run":
					p.AppRunID = ""
				case "not-ok":
					p.Status = "starting"
				case "legacy":
					p.StartedAt = time.Time{}
					p.AppRunID = ""
				case "http-error":
					w.WriteHeader(500)
					return
				case "redirect":
					http.Redirect(w, r, target.URL, http.StatusFound)
					return
				case "changed-after":
					changed := info
					changed.AppRunID = "successor"
					if err := runfile.Write(cfg.runFile, changed); err != nil {
						t.Error(err)
					}
				case "removed":
					if err := os.Remove(cfg.runFile); err != nil {
						t.Error(err)
					}
				case "corrupt":
					if err := os.WriteFile(cfg.runFile, []byte("{"), 0600); err != nil {
						t.Error(err)
					}
				case "timeout":
					time.Sleep(30 * time.Millisecond)
				}
				_ = json.NewEncoder(w).Encode(p)
			}))
			defer srv.Close()
			info.Port = serverPort(t, srv.URL)
			if name == "missing-record-start" {
				info.StartedAt = time.Time{}
			}
			if name == "missing-record-run" {
				info.AppRunID = ""
			}
			if err := runfile.Write(cfg.runFile, info); err != nil {
				t.Fatal(err)
			}
			client := &http.Client{}
			if name == "network-denied" {
				client.Transport = identityRoundTripper(func(*http.Request) (*http.Response, error) { return nil, syscall.EPERM })
			}
			if name == "timeout" {
				client.Timeout = 5 * time.Millisecond
			}
			c := &commandContext{deps: Deps{HTTPClient: client, ProcessAlive: func(int) bool {
				if name == "changed-before" {
					changed := info
					changed.AppRunID = "successor"
					if err := runfile.Write(cfg.runFile, changed); err != nil {
						t.Error(err)
					}
				}
				return false
			}}}
			if name == "connection-refused" {
				srv.Close()
			}
			err := c.postJSON(context.Background(), "sessions/worker/reviews/submit", struct{}{}, nil)
			if name == "matching" {
				if err != nil || posts != 1 || probes != 1 {
					t.Fatalf("err=%v posts=%d probes=%d", err, posts, probes)
				}
			} else if name == "post-redirect" {
				if err == nil || posts != 1 {
					t.Fatalf("redirect err=%v posts=%d", err, posts)
				}
			} else if err == nil || posts != 0 {
				t.Fatalf("unverified callback sent: err=%v posts=%d", err, posts)
			}
			if redirects != 0 {
				t.Fatalf("followed redirect %d times", redirects)
			}
			if name == "legacy" && !strings.Contains(err.Error(), "older daemon") {
				t.Fatalf("compatibility error=%v", err)
			}
			if name == "network-denied" && !errors.Is(err, syscall.EPERM) {
				t.Fatalf("network rejection lost: %v", err)
			}
		})
	}
}

type identityRoundTripper func(*http.Request) (*http.Response, error)

func (f identityRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestInvisiblePIDReviewSameResultRetry(t *testing.T) {
	cfg := setReviewEnv(t)
	info := runfile.Info{PID: 4242, StartedAt: time.Unix(100, 42).UTC(), AppRunID: "launch"}
	requests, effects := 0, 0
	var first string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			_ = json.NewEncoder(w).Encode(probeResult{Status: "ok", Service: daemonmeta.ServiceName, PID: info.PID, StartedAt: info.StartedAt, AppRunID: info.AppRunID})
			return
		}
		if r.URL.Path == "/internal/telemetry/cli-invoked" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.URL.Path != "/api/v1/sessions/worker/reviews/submit" {
			t.Error("unexpected endpoint", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		requests++
		if requests == 1 {
			first = string(raw)
			effects++
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		if string(raw) != first {
			t.Error("retry changed original result")
			effects++
		}
		_, _ = io.WriteString(w, `{"reviews":[{"id":"run-1","verdict":"approved"}]}`)
	}))
	defer srv.Close()
	info.Port = serverPort(t, srv.URL)
	if err := runfile.Write(cfg.runFile, info); err != nil {
		t.Fatal(err)
	}
	out, _, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return false }, In: strings.NewReader(`{"reviews":[{"runId":"run-1","verdict":"approved","githubReviewId":"123","body":"synthetic"}]}`)}, "review", "submit", "--session", "worker", "--reviews", "-")
	if err != nil || requests != 2 || effects != 1 || !strings.Contains(out, "recorded 1 review") {
		t.Fatalf("err=%v requests=%d effects=%d out=%s", err, requests, effects, out)
	}
}

func TestInvisiblePIDAPIErrorNotRetried(t *testing.T) {
	cfg := setReviewEnv(t)
	info := runfile.Info{PID: 4242, StartedAt: time.Unix(100, 42).UTC(), AppRunID: "launch"}
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			_ = json.NewEncoder(w).Encode(probeResult{Status: "ok", Service: daemonmeta.ServiceName, PID: info.PID, StartedAt: info.StartedAt, AppRunID: info.AppRunID})
			return
		}
		if r.URL.Path == "/internal/telemetry/cli-invoked" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		posts++
		w.WriteHeader(422)
		_, _ = io.WriteString(w, `{"code":"REVIEW_INVALID","message":"not submittable"}`)
	}))
	defer srv.Close()
	info.Port = serverPort(t, srv.URL)
	if err := runfile.Write(cfg.runFile, info); err != nil {
		t.Fatal(err)
	}
	_, _, err := executeCLI(t, Deps{ProcessAlive: func(int) bool { return false }}, "review", "submit", "worker", "--run", "run-1", "--verdict", "approved")
	if err == nil || posts != 1 || !strings.Contains(err.Error(), "REVIEW_INVALID") {
		t.Fatalf("err=%v posts=%d", err, posts)
	}
}

func TestHostVisiblePIDPreservesLegacyCallback(t *testing.T) {
	cfg := setReviewEnv(t)
	srv, _ := reviewServer(t, 200, `{"review":{"id":"run-1","verdict":"approved"}}`)
	writeRunFileFor(t, cfg, srv)
	_, _, err := executeCLI(t, aliveDeps(), "review", "submit", "worker", "--run", "run-1", "--verdict", "approved")
	if err != nil {
		t.Fatal(err)
	}
}

package cli

import (
	"bytes"
	"encoding/json"
	"github.com/spf13/cobra"
	"net/http"
	"strings"
	"testing"
)

func TestProviderProfilesCLIUseOnlyPatchesActive(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, capture := projectServer(t, http.StatusOK, `{"activeProviderProfile":"krill","profiles":[]}`)
	writeRunFileFor(t, cfg, srv)
	_, _, err := executeCLI(t, aliveDeps(), "project", "provider", "use", "krill", "--project", "demo", "--json")
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err = json.Unmarshal(capture.body, &body); err != nil {
		t.Fatal(err)
	}
	if capture.method != "PATCH" || capture.path != "/api/v1/projects/demo/provider-profiles" || len(body) != 1 || body["activeProviderProfile"] != "krill" {
		t.Fatalf("unexpected request %s %s %s", capture.method, capture.path, capture.body)
	}
}

func TestProviderProfilesCLIListAndSessionLabel(t *testing.T) {
	cfg := setConfigEnv(t)
	srv, _ := projectServer(t, http.StatusOK, `{"activeProviderProfile":"krill","profiles":[{"name":"krill","label":"krill","roles":["worker","reviewer"],"envKeys":["CODEX_HOME"]}]}`)
	writeRunFileFor(t, cfg, srv)
	out, _, err := executeCLI(t, aliveDeps(), "project", "provider", "ls", "--project", "demo")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "krill: krill [worker,reviewer]") {
		t.Fatal(out)
	}
}

func TestProviderProfilesMergeRequiresURLAndExpectedHead(t *testing.T) {
	setConfigEnv(t)
	for _, args := range [][]string{{"pr", "merge", "42"}, {"pr", "merge", "42", "--pr-url", "https://github.com/acme/repo/pull/42"}, {"pr", "merge", "42", "--expected-head", "abcdef"}} {
		_, _, err := executeCLI(t, aliveDeps(), args...)
		if ExitCode(err) != 2 {
			t.Fatalf("missing guard accepted: %v", err)
		}
	}
}

func TestProviderProfilesCLISessionDetails(t *testing.T) {
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetOut(&out)
	if err := writeSessionDetails(cmd, sessionDTO{ID: "demo-1", ProviderProfile: "krill", ProviderLabel: "krill"}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "provider profile: krill") || !strings.Contains(out.String(), "provider label: krill") {
		t.Fatal(out.String())
	}
}

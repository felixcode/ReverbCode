package persistenthost

import (
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

func TestDisconnectIdentityDistinguishesProviderEndAndUnknownOwner(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux process birth fixture")
	}
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _ = child.Wait() })
	root := t.TempDir()
	d := Descriptor{Version: ProtocolVersion, SessionID: "identity", Protocol: ProtocolACP, Address: "127.0.0.1:1", PID: os.Getpid(), Token: "synthetic", OwnershipFingerprint: "synthetic-owner", StartedAt: time.Now()}
	if err := writeDescriptor(root, d); err != nil {
		t.Fatal(err)
	}
	probe := captureDisconnectState(d, []string{root})
	if got := probe(); got != DisconnectPreserved {
		t.Fatalf("same live host/provider: %s", got)
	}
	altered := d
	altered.Token = "other-owner"
	if err := writeDescriptor(root, altered); err != nil {
		t.Fatal(err)
	}
	if got := probe(); got != DisconnectUnknown {
		t.Fatalf("changed owner: %s", got)
	}
	if err := writeDescriptor(root, d); err != nil {
		t.Fatal(err)
	}
	_ = child.Process.Kill()
	_ = child.Wait()
	if got := probe(); got != DisconnectEnded {
		t.Fatalf("host alive/provider dead: %s", got)
	}
}

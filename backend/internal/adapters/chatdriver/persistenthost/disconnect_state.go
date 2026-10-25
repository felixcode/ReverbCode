package persistenthost

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type DisconnectState string

const (
	DisconnectPreserved DisconnectState = "preserved"
	DisconnectEnded     DisconnectState = "ended"
	DisconnectUnknown   DisconnectState = "unknown"
)

type processIdentity struct {
	pid, parent int
	birth       string
}

func inspectProcess(pid int) (processIdentity, bool, error) {
	if runtime.GOOS != "linux" {
		return processIdentity{}, false, fmt.Errorf("process birth proof unavailable")
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if os.IsNotExist(err) {
		return processIdentity{}, false, nil
	}
	if err != nil {
		return processIdentity{}, false, err
	}
	end := strings.LastIndex(string(b), ")")
	if end < 0 {
		return processIdentity{}, false, fmt.Errorf("unknown process identity")
	}
	f := strings.Fields(string(b[end+1:]))
	if len(f) < 20 {
		return processIdentity{}, false, fmt.Errorf("unknown process identity")
	}
	parent, err := strconv.Atoi(f[1])
	if err != nil {
		return processIdentity{}, false, err
	}
	return processIdentity{pid: pid, parent: parent, birth: f[19]}, f[0] != "Z" && f[0] != "X", nil
}
func captureDisconnectState(d Descriptor, dataDirs []string) func() DisconnectState {
	host, _, hostErr := inspectProcess(d.PID)
	var provider processIdentity
	var providerErr error
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/children", d.PID, d.PID))
	children := strings.Fields(string(b))
	if err == nil && len(children) == 1 {
		pid, e := strconv.Atoi(children[0])
		if e == nil {
			provider, _, providerErr = inspectProcess(pid)
		} else {
			providerErr = e
		}
	} else {
		providerErr = fmt.Errorf("provider identity unknown")
	}
	return func() DisconnectState {
		current, alive, err := inspectProcess(d.PID)
		if err != nil {
			return DisconnectUnknown
		}
		if !alive {
			return DisconnectEnded
		}
		if hostErr != nil || host.birth == "" {
			return DisconnectUnknown
		}
		if current.birth != host.birth {
			return DisconnectEnded
		}
		if len(dataDirs) != 1 {
			return DisconnectUnknown
		}
		registered, err := readDescriptor(dataDirs[0], d.SessionID)
		if err != nil || registered.Version != d.Version || registered.SessionID != d.SessionID || registered.Address != d.Address || registered.PID != d.PID || registered.Token != d.Token || registered.OwnershipFingerprint != d.OwnershipFingerprint || registered.Protocol != d.Protocol || !registered.StartedAt.Equal(d.StartedAt) {
			return DisconnectUnknown
		}
		if providerErr != nil || provider.birth == "" {
			return DisconnectUnknown
		}
		child, alive, err := inspectProcess(provider.pid)
		if err != nil {
			return DisconnectUnknown
		}
		if !alive || child.birth != provider.birth {
			return DisconnectEnded
		}
		if child.parent != d.PID {
			return DisconnectUnknown
		}
		return DisconnectPreserved
	}
}

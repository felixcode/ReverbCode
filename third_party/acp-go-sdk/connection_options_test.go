package acp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestNotificationOptionsEnforceQueueAndActiveByteBounds(t *testing.T) {
	const frame = `{"jsonrpc":"2.0","method":"test/notify","params":{}}`
	for _, tc := range []struct {
		name                string
		options             ConnectionOptions
		acceptedAfterActive int
	}{
		{"queue", ConnectionOptions{MaxQueuedNotifications: 2, MaxNotificationBytes: 4096}, 2},
		{"bytes-include-active", ConnectionOptions{MaxQueuedNotifications: 8, MaxNotificationBytes: int64(len(frame))}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input, writer := io.Pipe()
			t.Cleanup(func() { _ = input.Close(); _ = writer.Close() })
			started, release := make(chan struct{}), make(chan struct{})
			first := true
			c := NewConnectionWithOptions(func(context.Context, string, json.RawMessage) (any, *RequestError) {
				if first {
					first = false
					close(started)
					<-release
				}
				return nil, nil
			}, io.Discard, input, tc.options)
			if _, err := io.WriteString(writer, frame+"\n"); err != nil {
				t.Fatal(err)
			}
			<-started
			for i := 0; i < tc.acceptedAfterActive; i++ {
				if _, err := io.WriteString(writer, frame+"\n"); err != nil {
					t.Fatal(err)
				}
			}
			waitForAcceptedNotifications(t, c, uint64(tc.acceptedAfterActive+1))
			c.notifyMu.Lock()
			before := c.pendingNotificationBytes
			c.notifyMu.Unlock()
			if before != int64((tc.acceptedAfterActive+1)*len(frame)) {
				t.Fatalf("retained bytes=%d", before)
			}
			if _, err := io.WriteString(writer, frame+"\n"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.Done():
			case <-time.After(time.Second):
				t.Fatal("budget failed to close connection")
			}
			if !errors.Is(context.Cause(c.ctx), errNotificationQueueOverflow) {
				t.Fatalf("cause=%v", context.Cause(c.ctx))
			}
			close(release)
			waitForNotificationBarrierDrain(t, c, time.Second)
			c.notifyMu.Lock()
			remaining := c.pendingNotificationBytes
			c.notifyMu.Unlock()
			if remaining != 0 {
				t.Fatalf("notification byte charge leaked: %d", remaining)
			}
		})
	}
}

func TestNotificationOptionsAllowControlRequestWhileHandlerBlocked(t *testing.T) {
	input, writer := io.Pipe()
	output, reader := io.Pipe()
	t.Cleanup(func() { _ = input.Close(); _ = writer.Close(); _ = output.Close(); _ = reader.Close() })
	started, release := make(chan struct{}), make(chan struct{})
	c := NewConnectionWithOptions(func(_ context.Context, method string, _ json.RawMessage) (any, *RequestError) {
		if method == "test/notify" {
			close(started)
			<-release
		}
		return map[string]bool{"ok": true}, nil
	}, reader, input, ConnectionOptions{MaxQueuedNotifications: 16, MaxNotificationBytes: 4096})
	if _, err := io.WriteString(writer, `{"jsonrpc":"2.0","method":"test/notify","params":{}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	<-started
	result := make(chan string, 1)
	go func() { line, _ := bufio.NewReader(output).ReadString('\n'); result <- line }()
	if _, err := io.WriteString(writer, `{"jsonrpc":"2.0","id":7,"method":"test/control","params":{}}`+"\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case line := <-result:
		if !strings.Contains(line, `"ok":true`) {
			t.Fatalf("control response=%s", line)
		}
	case <-time.After(time.Second):
		t.Fatal("notification handler blocked control response")
	}
	close(release)
	waitForNotificationBarrierDrain(t, c, time.Second)
}

func TestNotificationByteBudgetReleasesAfterEachHandler(t *testing.T) {
	const frame = `{"jsonrpc":"2.0","method":"test/notify","params":{}}`
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	c := NewConnectionWithOptions(func(context.Context, string, json.RawMessage) (any, *RequestError) { return nil, nil }, io.Discard, input, ConnectionOptions{MaxQueuedNotifications: 2, MaxNotificationBytes: int64(len(frame))})
	for i := 0; i < 16; i++ {
		if _, err := io.WriteString(writer, frame+"\n"); err != nil {
			t.Fatal(err)
		}
		waitForAcceptedNotifications(t, c, uint64(i+1))
		waitForNotificationBarrierDrain(t, c, time.Second)
		select {
		case <-c.Done():
			t.Fatal("completed handler did not free byte budget")
		default:
		}
	}
}

func waitForAcceptedNotifications(t *testing.T, c *Connection, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.notifyMu.Lock()
		got := c.lastEnqueuedNotificationSeq
		c.notifyMu.Unlock()
		if got >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("notification was not accepted")
}

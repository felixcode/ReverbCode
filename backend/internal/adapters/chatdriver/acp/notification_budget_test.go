package acp

import (
	"context"
	"encoding/json"
	acpsdk "github.com/coder/acp-go-sdk"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfiguredNotificationBudgetFitsReplayAndStillBoundsOverflow(t *testing.T) {
	for _, budget := range []int64{64 * 1024 * 1024, 2 * 1024 * 1024} {
		t.Run(strconv.FormatInt(budget, 10), func(t *testing.T) {
			t.Setenv("AO_ACP_NOTIFICATION_QUEUE_BYTES", strconv.FormatInt(budget, 10))
			t.Setenv("AO_ACP_NOTIFICATION_QUEUE_CAPACITY", "16384")
			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			release := make(chan struct{})
			defer close(release)
			var handled atomic.Int32
			conn := acpsdk.NewConnectionWithOptions(func(context.Context, string, json.RawMessage) (any, *acpsdk.RequestError) {
				<-release
				handled.Add(1)
				return nil, nil
			}, io.Discard, reader, notificationQueueOptions())
			frame := `{"jsonrpc":"2.0","method":"test/notification","params":{"text":"` + strings.Repeat("x", 1024*1024) + `"}}` + "\n"
			frames := 31
			if budget < 64*1024*1024 {
				frames = 3
			}
			sent := make(chan error, 1)
			go func() {
				for i := 0; i < frames; i++ {
					if _, err := io.WriteString(writer, frame); err != nil {
						sent <- err
						return
					}
				}
				sent <- nil
			}()
			select {
			case err := <-sent:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("bounded replay writer blocked")
			}
			if budget == 64*1024*1024 {
				select {
				case <-conn.Done():
					t.Fatal("64MiB disconnected on 31MiB replay")
				case <-time.After(50 * time.Millisecond):
				}
			} else {
				select {
				case <-conn.Done():
				case <-time.After(time.Second):
					t.Fatal("finite byte bound failed")
				}
			}
		})
	}
}

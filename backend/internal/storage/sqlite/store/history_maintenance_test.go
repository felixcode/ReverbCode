package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMaintenanceAdmissionYieldsToBusiness(t *testing.T) {
	m := newContextMutex()
	m.Lock()
	waiting := make(chan struct{})
	done := make(chan struct{})
	go func() { close(waiting); m.Lock(); defer m.Unlock(); close(done) }()
	<-waiting
	until := time.Now().Add(time.Second)
	for !m.BusinessWaiting() && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	if m.TryMaintenance() {
		t.Fatal("maintenance admitted before waiting business")
	}
	m.Unlock()
	<-done
	if !m.TryMaintenance() {
		t.Fatal("idle maintenance refused")
	}
	m.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(m.LockContext(ctx), context.Canceled) {
		t.Fatal("canceled business lock")
	}
	if m.BusinessWaiting() {
		t.Fatal("waiter leaked")
	}
}
func TestMaintenanceStrictPolicies(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `{"a":{"b":1,"b":2}}`, `{"a":"\ud800"}`, `{"a":"\udc00"}`, `{"n":NaN}`, `{} {}`} {
		if _, err := maintenanceJSON(raw); err == nil {
			t.Fatalf("accepted invalid JSON fixture %q", raw)
		}
	}
	for _, raw := range []string{`{"a":"\ud83d\ude00"}`, `{"a":"literal \\ud800"}`, `{"a":1e3,"b":[null,false]}`} {
		if _, err := maintenanceJSON(raw); err != nil {
			t.Fatalf("valid JSON: %v", err)
		}
	}
	cutoff := time.Date(2026, 10, 24, 0, 0, 0, 123456789, time.UTC)
	for _, raw := range []string{"2026-10-24T00:00:00.123456789Z", "2026-10-24 00:00:00.123456789 +0000 UTC", "2026-10-24 00:00:00.123456789"} {
		old, err := maintenanceOld(raw, cutoff, true)
		if !old || err != nil {
			t.Fatalf("nanoseconds: %s %v", raw, err)
		}
	}
	for _, raw := range []string{"2026-10-24T00:00:00", "0001-01-01T00:00:00Z", "2026-10-24 00:00:00 +0100 UTC"} {
		if _, err := maintenanceTime(raw); err == nil {
			t.Fatalf("unproven time accepted %s", raw)
		}
	}
	original := maintenanceRow{"role": "user", "text": "unique-expired-sentinel", "delivery_content_json": "body"}
	update, fields, err := maintenanceTransform(original, "message", "now", "cutoff")
	if err != nil || update["text"] != maintenanceExpired || len(fields) != 2 {
		t.Fatal(update, fields, err)
	}
	raw := maintenanceRow{"payload_json": `{"method":"turn.completed","providerTurnId":"native","error":null,"text":"secret","usage":{"input":4}}`}
	update, fields, err = maintenanceTransform(raw, "raw", "now", "cutoff")
	if err != nil || len(fields) != 1 {
		t.Fatal(err)
	}
	v, err := maintenanceJSON(update["payload_json"].(string))
	if err != nil {
		t.Fatal(err)
	}
	m := v.(map[string]any)
	if m["error"] != nil || m["providerTurnId"] != "native" || m["text"] != "" {
		t.Fatal("authentication facts changed")
	}
}

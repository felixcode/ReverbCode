package acp

import "testing"

func TestNotificationQueueOptions(t *testing.T) {
	for _, tc := range []struct {
		capacity, bytes string
		wantCapacity    int
		wantBytes       int64
	}{
		{"", "", 16384, 16 * 1024 * 1024}, {"2048", "4194304", 2048, 4194304}, {"invalid", "-1", 16384, 16 * 1024 * 1024}, {"0", "0", 16384, 16 * 1024 * 1024},
	} {
		t.Run(tc.capacity+":"+tc.bytes, func(t *testing.T) {
			t.Setenv("AO_ACP_NOTIFICATION_QUEUE_CAPACITY", tc.capacity)
			t.Setenv("AO_ACP_NOTIFICATION_QUEUE_BYTES", tc.bytes)
			got := notificationQueueOptions()
			if got.MaxQueuedNotifications != tc.wantCapacity || got.MaxNotificationBytes != tc.wantBytes {
				t.Fatalf("options=%+v", got)
			}
		})
	}
}

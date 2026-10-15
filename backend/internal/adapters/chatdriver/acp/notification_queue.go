package acp

import (
	acpsdk "github.com/coder/acp-go-sdk"
	"os"
	"strconv"
)

// Wire-byte budget covers queued notifications and the active handler. The SDK
// still reads requests/responses independently and closes explicitly on overflow.
func notificationQueueOptions() acpsdk.ConnectionOptions {
	capacity := 16384
	if value, err := strconv.Atoi(os.Getenv("AO_ACP_NOTIFICATION_QUEUE_CAPACITY")); err == nil && value > 0 {
		capacity = value
	}
	bytes := int64(16 * 1024 * 1024)
	if value, err := strconv.ParseInt(os.Getenv("AO_ACP_NOTIFICATION_QUEUE_BYTES"), 10, 64); err == nil && value > 0 {
		bytes = value
	}
	return acpsdk.ConnectionOptions{MaxQueuedNotifications: capacity, MaxNotificationBytes: bytes}
}

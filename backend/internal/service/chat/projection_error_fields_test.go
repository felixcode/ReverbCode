package chat

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

type projectionLogFixture struct{}

func (*projectionLogFixture) Error() string                   { return "private synthetic payload sentinel" }
func (*projectionLogFixture) Code() int                       { return 517 }
func (*projectionLogFixture) ProviderProjectionPhase() string { return "archive" }
func TestProjectionFailureFieldsAreBoundedMetadataOnly(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", &projectionLogFixture{})
	want := []any{"phase", "archive", "attempts", 4, "duration_ms", int64(37), "sqlite_primary_code", 5, "sqlite_extended_code", 517}
	if got := projectionFailureLogFields(err, "projection", 4, 37*time.Millisecond); !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata mismatch: %v", got)
	}
	got := projectionFailureLogFields(err, "ack", 1, time.Millisecond)
	if got[1] != "ack" {
		t.Fatal("ACK failure mislabeled as DB projection")
	}
	uncoded := projectionFailureLogFields(fmt.Errorf("private synthetic payload"), "projection", 1, 0)
	if len(uncoded) != 6 {
		t.Fatal("unknown driver code emitted as SQLite success/extra content")
	}
}

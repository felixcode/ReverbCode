package chat

import (
	"errors"
	"time"
)

// Operational metadata is added only to the existing fatal-projection log. Do
// not import the storage driver or expose SQL, DSNs, prompts or event payloads.
func projectionFailureLogFields(err error, phase string, attempts int, elapsed time.Duration) []any {
	var staged interface{ ProviderProjectionPhase() string }
	if phase != "ack" && errors.As(err, &staged) {
		phase = staged.ProviderProjectionPhase()
	}
	fields := []any{"phase", phase, "attempts", attempts, "duration_ms", elapsed.Milliseconds()}
	var coded interface{ Code() int }
	if phase != "ack" && errors.As(err, &coded) {
		code := coded.Code()
		fields = append(fields, "sqlite_primary_code", code&255, "sqlite_extended_code", code)
	}
	return fields
}

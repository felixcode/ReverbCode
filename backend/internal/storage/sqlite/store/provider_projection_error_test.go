package store

import (
	"errors"
	"fmt"
	"testing"
)

type projectionCodedFixture struct{}

func (*projectionCodedFixture) Error() string { return "synthetic driver error" }
func (*projectionCodedFixture) Code() int     { return 517 }
func TestProjectionStagePreservesDriverErrorAndOriginalMessage(t *testing.T) {
	underlying := &projectionCodedFixture{}
	original := fmt.Errorf("archive provider event synthetic: %w", underlying)
	err := projectionFailure("archive", original)
	if err.Error() != original.Error() || !errors.Is(err, underlying) {
		t.Fatal("classification or original error changed")
	}
	var staged interface{ ProviderProjectionPhase() string }
	var coded interface{ Code() int }
	if !errors.As(err, &staged) || staged.ProviderProjectionPhase() != "archive" || !errors.As(err, &coded) || coded.Code() != 517 {
		t.Fatal("safe phase/code lost across wrapped error")
	}
}

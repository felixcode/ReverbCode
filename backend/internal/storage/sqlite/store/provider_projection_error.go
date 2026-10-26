package store

// projectionError adds an internal operational stage without changing the
// original error text, classification or errors.Is/As chain.
type projectionError struct {
	phase string
	err   error
}

func (e *projectionError) Error() string                   { return e.err.Error() }
func (e *projectionError) Unwrap() error                   { return e.err }
func (e *projectionError) ProviderProjectionPhase() string { return e.phase }
func projectionFailure(phase string, err error) error {
	if err == nil {
		return nil
	}
	return &projectionError{phase: phase, err: err}
}

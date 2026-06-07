package zapwire

import "errors"

var (
	// ErrNoTransport is returned by New when transport is nil.
	ErrNoTransport = errors.New("zapwire: transport is required")
	// ErrNoEncoder is returned by New when encoder is nil.
	ErrNoEncoder = errors.New("zapwire: encoder is required")
	// ErrNoFramer is returned by New when framer is nil.
	ErrNoFramer = errors.New("zapwire: framer is required")
)

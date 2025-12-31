package api

import "errors"

// Package-level errors for the API server.
var (
	// ErrServerAlreadyStarted is returned when Start is called on an already running server.
	ErrServerAlreadyStarted = errors.New("api: server already started")

	// ErrServerNotStarted is returned when operations require a running server.
	ErrServerNotStarted = errors.New("api: server not started")

	// ErrInvalidListenAddr is returned when the listen address is malformed.
	ErrInvalidListenAddr = errors.New("api: invalid listen address")

	// ErrInvalidTimeout is returned when a timeout value is invalid.
	ErrInvalidTimeout = errors.New("api: invalid timeout value (must be positive)")

	// ErrInvalidCORSOrigin is returned when a CORS origin is malformed.
	ErrInvalidCORSOrigin = errors.New("api: invalid CORS origin")

	// ErrShutdownTimeout is returned when graceful shutdown times out.
	ErrShutdownTimeout = errors.New("api: shutdown timed out")
)

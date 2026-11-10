package api

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"time"
)

// Config holds the configuration for the API server.
type Config struct {
	// ListenAddr is the address to listen on (e.g., "127.0.0.1:8080" or ":8080").
	// Default: "127.0.0.1:8080"
	ListenAddr string

	// AuthToken is the Bearer token for API authentication.
	// If empty, authentication is disabled.
	// Default: "" (disabled)
	AuthToken string

	// CORSOrigins is the list of allowed CORS origins.
	// If empty, CORS is disabled.
	// Use "*" to allow all origins (not recommended for production).
	// Default: nil (disabled)
	CORSOrigins []string

	// ReadTimeout is the maximum duration for reading the entire request.
	// Default: 30s
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration for writing the response.
	// Default: 30s
	WriteTimeout time.Duration

	// IdleTimeout is the maximum duration to wait for the next request.
	// Default: 60s
	IdleTimeout time.Duration

	// ShutdownTimeout is the maximum duration to wait for graceful shutdown.
	// Default: 10s
	ShutdownTimeout time.Duration

	// RateLimitEnabled enables rate limiting middleware.
	// Default: false
	RateLimitEnabled bool

	// RateLimitRequestsPerSecond is the sustained rate limit per client.
	// Default: 100
	RateLimitRequestsPerSecond float64

	// RateLimitBurst is the maximum burst size (token bucket capacity).
	// Default: 200
	RateLimitBurst int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ListenAddr:                 "127.0.0.1:8080",
		AuthToken:                  "",
		CORSOrigins:                nil,
		ReadTimeout:                30 * time.Second,
		WriteTimeout:               30 * time.Second,
		IdleTimeout:                60 * time.Second,
		ShutdownTimeout:            10 * time.Second,
		RateLimitEnabled:           false,
		RateLimitRequestsPerSecond: 100,
		RateLimitBurst:             200,
	}
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	// Validate listen address
	if c.ListenAddr == "" {
		return fmt.Errorf("%w: listen address cannot be empty", ErrInvalidListenAddr)
	}

	// Check if it's a valid host:port format
	host, port, err := net.SplitHostPort(c.ListenAddr)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidListenAddr, err)
	}

	// If host is specified, validate it
	if host != "" {
		ip := net.ParseIP(host)
		if ip == nil {
			// Not an IP, check if it could be a hostname
			if strings.ContainsAny(host, " \t\n\r") {
				return fmt.Errorf("%w: invalid host %q", ErrInvalidListenAddr, host)
			}
		}
	}

	// Port should be a number or empty (for dynamic allocation)
	if port != "" && port != "0" {
		// net.SplitHostPort already validated it's a valid port string
		_ = port
	}

	// Validate timeouts
	if c.ReadTimeout < 0 {
		return fmt.Errorf("%w: read timeout must be >= 0", ErrInvalidTimeout)
	}
	if c.WriteTimeout < 0 {
		return fmt.Errorf("%w: write timeout must be >= 0", ErrInvalidTimeout)
	}
	if c.IdleTimeout < 0 {
		return fmt.Errorf("%w: idle timeout must be >= 0", ErrInvalidTimeout)
	}
	if c.ShutdownTimeout < 0 {
		return fmt.Errorf("%w: shutdown timeout must be >= 0", ErrInvalidTimeout)
	}

	// Validate CORS origins
	for _, origin := range c.CORSOrigins {
		if origin == "*" {
			continue // Wildcard is allowed
		}
		if origin == "" {
			return fmt.Errorf("%w: empty origin", ErrInvalidCORSOrigin)
		}
		// Parse as URL to validate format
		u, err := url.Parse(origin)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidCORSOrigin, err)
		}
		if u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("%w: origin must have scheme and host: %q", ErrInvalidCORSOrigin, origin)
		}
	}

	// Validate rate limit configuration
	if c.RateLimitEnabled {
		if c.RateLimitRequestsPerSecond <= 0 {
			return fmt.Errorf("%w: requests per second must be > 0", ErrInvalidRateLimit)
		}
		if c.RateLimitBurst <= 0 {
			return fmt.Errorf("%w: burst must be > 0", ErrInvalidRateLimit)
		}
	}

	return nil
}

// LoopbackOnly reports whether ListenAddr accepts connections only from this host.
func (c *Config) LoopbackOnly() bool {
	host, _, err := net.SplitHostPort(c.ListenAddr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// CheckExposure refuses a config that lets other hosts, or any web page, read the API without an auth token.
func (c *Config) CheckExposure() error {
	if c.AuthToken != "" {
		return nil
	}
	if !c.LoopbackOnly() {
		return fmt.Errorf("%w: %s", ErrUnauthenticatedRemote, c.ListenAddr)
	}
	if slices.Contains(c.CORSOrigins, "*") {
		return ErrWildcardCORSWithoutToken
	}
	return nil
}

// WithListenAddr returns a copy of the config with a new listen address.
func (c Config) WithListenAddr(addr string) Config {
	c.ListenAddr = addr
	return c
}

// WithAuthToken returns a copy of the config with a new auth token.
func (c Config) WithAuthToken(token string) Config {
	c.AuthToken = token
	return c
}

// WithCORSOrigins returns a copy of the config with new CORS origins.
func (c Config) WithCORSOrigins(origins ...string) Config {
	c.CORSOrigins = origins
	return c
}

// WithRateLimit returns a copy of the config with rate limiting enabled.
func (c Config) WithRateLimit(requestsPerSecond float64, burst int) Config {
	c.RateLimitEnabled = true
	c.RateLimitRequestsPerSecond = requestsPerSecond
	c.RateLimitBurst = burst
	return c
}

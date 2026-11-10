// Package client provides an HTTP client for the GoClusterFS API.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Client is an HTTP client for the GoClusterFS API.
type Client struct {
	host       string
	port       int
	baseURL    string
	token      string
	httpClient *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHost sets the API server host name or IP address.
func WithHost(host string) Option {
	return func(c *Client) {
		c.host = host
	}
}

// WithPort sets the API server port.
func WithPort(port int) Option {
	return func(c *Client) {
		c.port = port
	}
}

// WithToken sets the Bearer token for authentication.
func WithToken(token string) Option {
	return func(c *Client) {
		c.token = token
	}
}

// WithTimeout sets the HTTP client timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.httpClient.Timeout = timeout
	}
}

// New creates a new API client with the given options.
func New(opts ...Option) *Client {
	c := &Client{
		host: "localhost",
		port: 8080,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	c.baseURL = "http://" + net.JoinHostPort(strings.Trim(c.host, "[]"), strconv.Itoa(c.port))
	return c
}

// BaseURL returns the configured base URL.
func (c *Client) BaseURL() string {
	return c.baseURL
}

// doRequest performs an HTTP request and returns the response body.
func (c *Client) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	url := c.baseURL + path

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
			return nil, &APIError{
				StatusCode: resp.StatusCode,
				Message:    errResp.Error,
				Details:    errResp.Details,
			}
		}
		return nil, &APIError{
			StatusCode: resp.StatusCode,
			Message:    http.StatusText(resp.StatusCode),
		}
	}

	return body, nil
}

// Get performs a GET request and decodes the JSON response into v.
func (c *Client) Get(ctx context.Context, path string, v interface{}) error {
	body, err := c.doRequest(ctx, http.MethodGet, path)
	if err != nil {
		return err
	}
	if v != nil {
		if err := json.Unmarshal(body, v); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

// GetRaw performs a GET request and returns the raw response body.
func (c *Client) GetRaw(ctx context.Context, path string) ([]byte, error) {
	return c.doRequest(ctx, http.MethodGet, path)
}

// ErrorResponse represents an API error response.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
	Details string `json:"details,omitempty"`
}

// APIError represents an error returned by the API.
type APIError struct {
	StatusCode int
	Message    string
	Details    string
}

func (e *APIError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("%s: %s", e.Message, e.Details)
	}
	return e.Message
}

// IsNotFound returns true if the error is a 404 Not Found error.
func (e *APIError) IsNotFound() bool {
	return e.StatusCode == http.StatusNotFound
}

// IsUnauthorized returns true if the error is a 401 Unauthorized error.
func (e *APIError) IsUnauthorized() bool {
	return e.StatusCode == http.StatusUnauthorized
}

// IsServerError returns true if the error is a 5xx server error.
func (e *APIError) IsServerError() bool {
	return e.StatusCode >= 500
}

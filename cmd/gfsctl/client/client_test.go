package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBaseURL(t *testing.T) {
	for _, tc := range []struct {
		host string
		port int
		want string
	}{
		{"localhost", 8080, "http://localhost:8080"},
		{"10.0.0.5", 9090, "http://10.0.0.5:9090"},
		{"::1", 8080, "http://[::1]:8080"},
		{"[::1]", 8080, "http://[::1]:8080"},
	} {
		if got := New(WithHost(tc.host), WithPort(tc.port)).BaseURL(); got != tc.want {
			t.Errorf("host %q port %d: BaseURL %q, want %q", tc.host, tc.port, got, tc.want)
		}
	}
	if got := New(WithPort(9000), WithHost("h")).BaseURL(); got != "http://h:9000" {
		t.Errorf("option order changed the URL: %q", got)
	}
}

func TestAPIErrorCarriesStatusAndDetails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"invalid token","code":401}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("not json"))
	}))
	defer srv.Close()
	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}

	var apiErr *APIError
	if _, err := c.GetRaw(context.Background(), "/metrics"); !errors.As(err, &apiErr) || !apiErr.IsUnauthorized() || apiErr.Message != "invalid token" {
		t.Fatalf("without token: %v", err)
	}
	c.token = "secret"
	if _, err := c.GetRaw(context.Background(), "/metrics"); !errors.As(err, &apiErr) || !apiErr.IsServerError() || apiErr.Message != "Service Unavailable" {
		t.Fatalf("non-JSON 503: %v", err)
	}
}

func TestOversizedResponseIsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := make([]byte, 1<<20)
		for written := 0; written <= MaxResponseSize; written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	c := &Client{baseURL: srv.URL, httpClient: srv.Client()}
	if _, err := c.GetRaw(context.Background(), "/metrics"); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Fatalf("oversized response: %v", err)
	}
}

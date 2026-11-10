package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

func TestAggregateHealth_AllOK(t *testing.T) {
	checkers := []HealthChecker{
		NewHealthCheckFunc("cache", func(ctx context.Context) ComponentHealth {
			return ComponentHealth{Status: HealthStatusOK}
		}),
		NewHealthCheckFunc("journal", func(ctx context.Context) ComponentHealth {
			return ComponentHealth{Status: HealthStatusOK}
		}),
	}

	resp := AggregateHealth(context.Background(), checkers...)

	if resp.Status != HealthStatusOK {
		t.Errorf("expected OK, got %s", resp.Status)
	}
	if len(resp.Checks) != 2 {
		t.Errorf("expected 2 checks, got %d", len(resp.Checks))
	}
}

func TestAggregateHealth_OneDegraded(t *testing.T) {
	checkers := []HealthChecker{
		NewHealthCheckFunc("cache", func(ctx context.Context) ComponentHealth {
			return ComponentHealth{Status: HealthStatusOK}
		}),
		NewHealthCheckFunc("journal", func(ctx context.Context) ComponentHealth {
			return ComponentHealth{Status: HealthStatusDegraded, Message: "low disk space"}
		}),
	}

	resp := AggregateHealth(context.Background(), checkers...)

	if resp.Status != HealthStatusDegraded {
		t.Errorf("expected Degraded, got %s", resp.Status)
	}
	if resp.Checks["journal"].Message != "low disk space" {
		t.Errorf("expected message, got %q", resp.Checks["journal"].Message)
	}
}

func TestAggregateHealth_OneUnhealthy(t *testing.T) {
	checkers := []HealthChecker{
		NewHealthCheckFunc("cache", func(ctx context.Context) ComponentHealth {
			return ComponentHealth{Status: HealthStatusDegraded}
		}),
		NewHealthCheckFunc("journal", func(ctx context.Context) ComponentHealth {
			return ComponentHealth{Status: HealthStatusUnhealthy, Message: "write failure"}
		}),
	}

	resp := AggregateHealth(context.Background(), checkers...)

	if resp.Status != HealthStatusUnhealthy {
		t.Errorf("expected Unhealthy, got %s", resp.Status)
	}
}

func TestAggregateHealth_Empty(t *testing.T) {
	resp := AggregateHealth(context.Background())

	if resp.Status != HealthStatusOK {
		t.Errorf("expected OK for empty checkers, got %s", resp.Status)
	}
	if len(resp.Checks) != 0 {
		t.Errorf("expected no checks, got %d", len(resp.Checks))
	}
}

func TestHealthEndpoint_WithCheckers(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Register a degraded checker
	server.RegisterHealthChecker(NewHealthCheckFunc("test", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: HealthStatusDegraded, Message: "test warning"}
	}))

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/health")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	// Degraded returns 200 (only unhealthy returns 503)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if health.Status != HealthStatusDegraded {
		t.Errorf("expected degraded status, got %s", health.Status)
	}
	if health.Checks["test"].Message != "test warning" {
		t.Errorf("expected message, got %q", health.Checks["test"].Message)
	}
}

func TestHealthEndpoint_Unhealthy_Returns503(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Register an unhealthy checker
	server.RegisterHealthChecker(NewHealthCheckFunc("critical", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: HealthStatusUnhealthy, Message: "critical failure"}
	}))

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/health")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	// Unhealthy returns 503
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}

	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if health.Status != HealthStatusUnhealthy {
		t.Errorf("expected unhealthy status, got %s", health.Status)
	}
}

func TestHealthEndpoint_MultipleCheckers(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr(":0")
	server, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer error: %v", err)
	}

	// Register multiple checkers
	server.RegisterHealthChecker(NewHealthCheckFunc("cache", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: HealthStatusOK}
	}))
	server.RegisterHealthChecker(NewHealthCheckFunc("journal", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: HealthStatusOK}
	}))
	server.RegisterHealthChecker(NewHealthCheckFunc("device", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: HealthStatusOK}
	}))

	if err := server.Start(context.Background()); err != nil {
		t.Fatalf("Start error: %v", err)
	}
	defer server.Stop()
	time.Sleep(10 * time.Millisecond)

	resp, err := http.Get("http://" + server.Addr() + "/health")
	if err != nil {
		t.Fatalf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if len(health.Checks) != 3 {
		t.Errorf("expected 3 checks, got %d", len(health.Checks))
	}
}

func TestDefaultHealthChecker(t *testing.T) {
	checker := DefaultHealthChecker()
	if checker.Name() != "api" {
		t.Errorf("expected name 'api', got %q", checker.Name())
	}

	health := checker.CheckHealth(context.Background())
	if health.Status != HealthStatusOK {
		t.Errorf("expected OK, got %s", health.Status)
	}
}

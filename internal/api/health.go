package api

import "context"

// HealthChecker is the interface that components implement to report health.
type HealthChecker interface {
	// Name returns the component name for health reporting.
	Name() string

	// CheckHealth returns the health status of the component.
	// The context allows for timeout on health checks.
	CheckHealth(ctx context.Context) ComponentHealth
}

// HealthCheckFunc allows a function to implement HealthChecker.
type HealthCheckFunc struct {
	name string
	fn   func(ctx context.Context) ComponentHealth
}

// NewHealthCheckFunc creates a HealthChecker from a function.
func NewHealthCheckFunc(name string, fn func(ctx context.Context) ComponentHealth) HealthChecker {
	return &HealthCheckFunc{name: name, fn: fn}
}

func (h *HealthCheckFunc) Name() string {
	return h.name
}

func (h *HealthCheckFunc) CheckHealth(ctx context.Context) ComponentHealth {
	return h.fn(ctx)
}

// AggregateHealth runs all health checks and returns aggregate status.
// Returns HealthStatusOK if all checks pass.
// Returns HealthStatusDegraded if any check is degraded (but none unhealthy).
// Returns HealthStatusUnhealthy if any check is unhealthy.
func AggregateHealth(ctx context.Context, checkers ...HealthChecker) HealthResponse {
	checks := make(map[string]ComponentHealth)
	overall := HealthStatusOK

	for _, checker := range checkers {
		health := checker.CheckHealth(ctx)
		checks[checker.Name()] = health

		// Determine overall status (unhealthy > degraded > ok)
		switch health.Status {
		case HealthStatusUnhealthy:
			overall = HealthStatusUnhealthy
		case HealthStatusDegraded:
			if overall != HealthStatusUnhealthy {
				overall = HealthStatusDegraded
			}
		}
	}

	return HealthResponse{
		Status: overall,
		Checks: checks,
	}
}

// DefaultHealthChecker returns a checker that always reports healthy.
// This is used as a placeholder until real components are injected.
func DefaultHealthChecker() HealthChecker {
	return NewHealthCheckFunc("api", func(ctx context.Context) ComponentHealth {
		return ComponentHealth{Status: HealthStatusOK}
	})
}

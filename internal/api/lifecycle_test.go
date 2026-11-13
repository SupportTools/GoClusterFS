package api

import (
	"context"
	"errors"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var serverFrames = []string{
	"net/http.(*conn).serve",
	"net/http.(*Server).Serve",
	"internal/api.(*Server)",
	"internal/api.(*RateLimiter)",
}

func serverGoroutines() (int, string) {
	buf := make([]byte, 1<<20)
	stacks := string(buf[:runtime.Stack(buf, true)])
	var count int
	var found []string
	for _, g := range strings.Split(stacks, "\n\n") {
		for _, frame := range serverFrames {
			if strings.Contains(g, frame) {
				count++
				found = append(found, g)
				break
			}
		}
	}
	return count, strings.Join(found, "\n\n")
}

func expectNoLeftoverGoroutines(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		n, stacks := serverGoroutines()
		if n <= before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d server goroutines left after Stop, %d before Start:\n%s", n, before, stacks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startWithChecker(t *testing.T, cfg Config, checker HealthChecker) *Server {
	t.Helper()
	s, err := NewServer(cfg.WithListenAddr("127.0.0.1:0").WithRateLimit(100, 100))
	if err != nil {
		t.Fatal(err)
	}
	s.RegisterHealthChecker(checker)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

func getStatus(url string) int {
	resp, err := http.Get(url)
	if err != nil {
		return 0
	}
	closeBody(resp)
	return resp.StatusCode
}

func TestStopLetsRequestsInFlightFinish(t *testing.T) {
	before, _ := serverGoroutines()
	entered, release := make(chan struct{}), make(chan struct{})
	s := startWithChecker(t, DefaultConfig(), NewHealthCheckFunc("slow", func(ctx context.Context) ComponentHealth {
		close(entered)
		<-release
		return ComponentHealth{Status: HealthStatusOK}
	}))
	addr := s.Addr()

	got := make(chan int, 1)
	go func() { got <- getStatus("http://" + addr + "/health") }()
	<-entered

	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop() }()
	select {
	case err := <-stopped:
		t.Fatalf("Stop returned %v while a request was in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		_ = conn.Close()
		t.Error("the server accepted a new connection while stopping")
	}

	close(release)
	if code := <-got; code != http.StatusOK {
		t.Errorf("request in flight got %d, want 200", code)
	}
	if err := <-stopped; err != nil {
		t.Errorf("Stop = %v, want nil", err)
	}
	expectNoLeftoverGoroutines(t, before)
}

func TestStopCancelsRequestsThatOutlastTheTimeout(t *testing.T) {
	before, _ := serverGoroutines()
	cfg := DefaultConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond
	entered := make(chan struct{})
	var returned atomic.Bool
	s := startWithChecker(t, cfg, NewHealthCheckFunc("stuck", func(ctx context.Context) ComponentHealth {
		close(entered)
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		returned.Store(true)
		return ComponentHealth{Status: HealthStatusUnhealthy}
	}))

	go getStatus("http://" + s.Addr() + "/health")
	<-entered

	if err := s.Stop(); !errors.Is(err, ErrShutdownTimeout) {
		t.Errorf("Stop = %v, want ErrShutdownTimeout", err)
	}
	if !returned.Load() {
		t.Error("Stop returned before the cancelled handler did")
	}
	expectNoLeftoverGoroutines(t, before)
}

func TestStopIdleServer(t *testing.T) {
	before, _ := serverGoroutines()
	s := startWithChecker(t, DefaultConfig(), DefaultHealthChecker())
	if code := getStatus("http://" + s.Addr() + "/health"); code != http.StatusOK {
		t.Fatalf("GET /health = %d", code)
	}
	if err := s.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	if err := s.Stop(); err != nil {
		t.Errorf("second Stop = %v, want the first result", err)
	}
	if code := getStatus("http://" + s.Addr() + "/health"); code != 0 {
		t.Errorf("GET /health after Stop = %d, want a refused connection", code)
	}
	expectNoLeftoverGoroutines(t, before)
}

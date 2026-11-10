package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type fakeFiles struct {
	mu      sync.Mutex
	calls   []string
	err     error
	release chan struct{}
}

func (f *fakeFiles) record(call string) error {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	release := f.release
	f.mu.Unlock()
	if release != nil {
		<-release
	}
	return f.err
}

func (f *fakeFiles) lastCall() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return ""
	}
	return f.calls[len(f.calls)-1]
}

func (f *fakeFiles) ListSnapshots(_ context.Context, path string) ([]SnapshotInfo, error) {
	if err := f.record("list " + path); err != nil {
		return nil, err
	}
	return []SnapshotInfo{{ID: 1, Name: "s1", Inode: 3, State: "active"}}, nil
}
func (f *fakeFiles) CreateSnapshot(_ context.Context, path, name string) error {
	return f.record("create " + path + " " + name)
}
func (f *fakeFiles) DeleteSnapshot(_ context.Context, path, name string) error {
	return f.record("delete " + path + " " + name)
}
func (f *fakeFiles) CloneSnapshot(_ context.Context, path, name, dest string) error {
	return f.record("clonesnap " + path + " " + name + " " + dest)
}
func (f *fakeFiles) CloneFile(_ context.Context, path, dest string) error {
	return f.record("clone " + path + " " + dest)
}

type fakeAdmin struct{ ops []string }

func (a *fakeAdmin) FlushJournal() (string, error) {
	a.ops = append(a.ops, "flush")
	return "done", nil
}
func (a *fakeAdmin) ForceTrim() (string, error)  { a.ops = append(a.ops, "trim"); return "done", nil }
func (a *fakeAdmin) ClearCache() (string, error) { a.ops = append(a.ops, "cache"); return "done", nil }

func startWriteServer(t *testing.T, token string) (*Server, *fakeFiles, *fakeAdmin) {
	t.Helper()
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken(token))
	files, admin := &fakeFiles{}, &fakeAdmin{}
	srv.SetFileService(files)
	srv.SetAdminService(admin)
	return srv, files, admin
}

func call(t *testing.T, srv *Server, method, path, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, "http://"+srv.Addr()+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data)
}

var writeCalls = []struct{ method, path, body, want string }{
	{"POST", "/api/v1/snapshots", `{"path":"/f","name":"s1"}`, "create /f s1"},
	{"DELETE", "/api/v1/snapshots/s1?path=/f", "", "delete /f s1"},
	{"POST", "/api/v1/snapshots/s1/clone", `{"path":"/f","dest":"/c"}`, "clonesnap /f s1 /c"},
	{"POST", "/api/v1/clones", `{"path":"/f","dest":"/c"}`, "clone /f /c"},
}

func TestWritesNeedToken(t *testing.T) {
	srv, files, admin := startWriteServer(t, "")
	for _, c := range writeCalls {
		if status, body := call(t, srv, c.method, c.path, "", c.body); status != http.StatusForbidden || !strings.Contains(body, "-api-token") {
			t.Errorf("%s %s without token: %d %s", c.method, c.path, status, body)
		}
	}
	for _, op := range []string{"flush-journal", "force-trim", "clear-cache"} {
		if status, _ := call(t, srv, "POST", "/api/v1/admin/"+op, "", ""); status != http.StatusForbidden {
			t.Errorf("admin %s without token: %d", op, status)
		}
	}
	if len(files.calls) != 0 || len(admin.ops) != 0 {
		t.Fatalf("services ran without a token: %v %v", files.calls, admin.ops)
	}
	if status, _ := call(t, srv, "GET", "/api/v1/snapshots?path=/f", "", ""); status != http.StatusForbidden {
		t.Fatalf("list without a token on loopback: %d, want 403", status)
	}
}

func TestWritesWithToken(t *testing.T) {
	srv, files, admin := startWriteServer(t, "secret")
	for _, c := range writeCalls {
		if status, _ := call(t, srv, c.method, c.path, "", c.body); status != http.StatusUnauthorized {
			t.Errorf("%s %s with no Authorization: %d, want 401", c.method, c.path, status)
		}
		status, body := call(t, srv, c.method, c.path, "secret", c.body)
		if status != http.StatusOK && status != http.StatusCreated {
			t.Errorf("%s %s: %d %s", c.method, c.path, status, body)
		}
		if got := files.lastCall(); got != c.want {
			t.Errorf("%s %s called %q, want %q", c.method, c.path, got, c.want)
		}
	}
	for _, op := range []string{"flush-journal", "force-trim", "clear-cache"} {
		if status, body := call(t, srv, "POST", "/api/v1/admin/"+op, "secret", ""); status != http.StatusOK || !strings.Contains(body, op) {
			t.Errorf("admin %s: %d %s", op, status, body)
		}
	}
	if fmt.Sprint(admin.ops) != "[flush trim cache]" {
		t.Errorf("admin ops = %v", admin.ops)
	}
}

func TestWriteErrorsMapToStatus(t *testing.T) {
	srv, files, _ := startWriteServer(t, "secret")
	for _, tc := range []struct {
		err  error
		want int
	}{
		{syscall.ENOENT, http.StatusNotFound},
		{syscall.EEXIST, http.StatusConflict},
		{syscall.ENOTSUP, http.StatusConflict},
		{syscall.EINVAL, http.StatusBadRequest},
		{syscall.ENOSPC, http.StatusInsufficientStorage},
		{syscall.EIO, http.StatusInternalServerError},
		{fmt.Errorf("%w: bad", ErrInvalidPath), http.StatusBadRequest},
		{fmt.Errorf("snapshots are %w", ErrDisabled), http.StatusConflict},
		{fmt.Errorf("%w", ErrNotMounted), http.StatusServiceUnavailable},
		{syscall.ENOSYS, http.StatusNotImplemented},
	} {
		files.err = tc.err
		if status, _ := call(t, srv, "POST", "/api/v1/snapshots", "secret", `{"path":"/f","name":"s"}`); status != tc.want {
			t.Errorf("%v: status %d, want %d", tc.err, status, tc.want)
		}
	}
}

func TestWriteRequestValidation(t *testing.T) {
	srv, files, _ := startWriteServer(t, "secret")
	for _, tc := range []struct {
		body string
		want int
	}{
		{`{"path":"/f","name":"s","extra":1}`, http.StatusBadRequest},
		{`{"path":"/f","name":"s"}{}`, http.StatusBadRequest},
		{`not json`, http.StatusBadRequest},
		{`{"path":"` + strings.Repeat("a", maxWriteBody) + `"}`, http.StatusRequestEntityTooLarge},
	} {
		if status, _ := call(t, srv, "POST", "/api/v1/snapshots", "secret", tc.body); status != tc.want {
			t.Errorf("body %.40q: status %d, want %d", tc.body, status, tc.want)
		}
	}
	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{"PUT", "/api/v1/snapshots", http.StatusMethodNotAllowed},
		{"GET", "/api/v1/clones", http.StatusMethodNotAllowed},
		{"GET", "/api/v1/admin/clear-cache", http.StatusMethodNotAllowed},
		{"POST", "/api/v1/admin/format", http.StatusNotFound},
		{"POST", "/api/v1/snapshots/s1/rename", http.StatusNotFound},
		{"HEAD", "/api/v1/snapshots/s1/clone", http.StatusMethodNotAllowed},
		{"HEAD", "/api/v1/clones", http.StatusMethodNotAllowed},
		{"HEAD", "/api/v1/admin/flush-journal", http.StatusMethodNotAllowed},
	} {
		if status, _ := call(t, srv, tc.method, tc.path, "secret", "{}"); status != tc.want {
			t.Errorf("%s %s: status %d, want %d", tc.method, tc.path, status, tc.want)
		}
	}
	if len(files.calls) != 0 {
		t.Fatalf("invalid requests reached the service: %v", files.calls)
	}
}

func TestStopWaitsForWritesInFlight(t *testing.T) {
	cfg := DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("secret")
	cfg.ShutdownTimeout = 50 * time.Millisecond
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	files := &fakeFiles{release: make(chan struct{})}
	srv.SetFileService(files)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	go func() {
		req, _ := http.NewRequest("POST", "http://"+srv.Addr()+"/api/v1/snapshots", strings.NewReader(`{"path":"/f","name":"s"}`))
		req.Header.Set("Authorization", "Bearer secret")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	for files.lastCall() == "" {
		time.Sleep(5 * time.Millisecond)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- srv.Stop() }()

	select {
	case err := <-stopped:
		t.Fatalf("Stop returned (%v) while a write was still running", err)
	case <-time.After(300 * time.Millisecond):
	}
	close(files.release)
	select {
	case err := <-stopped:
		if err != nil && !errors.Is(err, ErrShutdownTimeout) {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return after the write finished")
	}
}

func TestWritesRefuseOtherOrigins(t *testing.T) {
	srv, err := NewServer(DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("secret").WithCORSOrigins("http://localhost:5173"))
	if err != nil {
		t.Fatal(err)
	}
	files := &fakeFiles{}
	srv.SetFileService(files)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Stop() })

	send := func(method, origin string, header map[string]string) *http.Response {
		req, _ := http.NewRequest(method, "http://"+srv.Addr()+"/api/v1/clones", strings.NewReader(`{"path":"/f","dest":"/c"}`))
		req.Header.Set("Authorization", "Bearer secret")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		for k, v := range header {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}

	for _, m := range []string{"POST", "DELETE"} {
		resp := send("OPTIONS", "http://localhost:5173", map[string]string{"Access-Control-Request-Method": m, "Access-Control-Request-Headers": "authorization"})
		if resp.StatusCode != http.StatusForbidden || resp.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Errorf("preflight for %s from an allowed origin: %d, Allow-Origin %q; want 403 and none", m, resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"))
		}
	}
	if resp := send("OPTIONS", "http://localhost:5173", map[string]string{"Access-Control-Request-Method": "GET"}); resp.StatusCode != http.StatusNoContent {
		t.Errorf("preflight for GET: %d, want 204", resp.StatusCode)
	}
	for _, origin := range []string{"http://localhost:5173", "http://evil.example", "null"} {
		if resp := send("POST", origin, nil); resp.StatusCode != http.StatusForbidden {
			t.Errorf("POST with Origin %q: %d, want 403", origin, resp.StatusCode)
		}
	}
	if resp := send("POST", "http://"+srv.Addr(), nil); resp.StatusCode != http.StatusCreated {
		t.Errorf("same-origin POST: %d, want 201", resp.StatusCode)
	}
	if files.lastCall() != "clone /f /c" || len(files.calls) != 1 {
		t.Errorf("service calls = %v, want only the same-origin clone", files.calls)
	}
}

func TestTokenInQueryIsIgnored(t *testing.T) {
	srv, files, _ := startWriteServer(t, "secret")
	if status, _ := call(t, srv, "POST", "/api/v1/clones?token=secret&access_token=secret", "", `{"path":"/f","dest":"/c"}`); status != http.StatusUnauthorized {
		t.Fatalf("token in the query: %d, want 401", status)
	}
	if len(files.calls) != 0 {
		t.Fatalf("service ran: %v", files.calls)
	}
}

func TestDeleteTakesEscapedNames(t *testing.T) {
	srv, files, _ := startWriteServer(t, "secret")
	if status, body := call(t, srv, "DELETE", "/api/v1/snapshots/nightly%2F2026%3A1?path=/f", "secret", ""); status != http.StatusOK {
		t.Fatalf("delete of an escaped name: %d %s", status, body)
	}
	if got := files.lastCall(); got != "delete /f nightly/2026:1" {
		t.Fatalf("service got %q", got)
	}
}

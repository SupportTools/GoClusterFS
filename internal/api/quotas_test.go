package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"syscall"
	"testing"
)

type fakeQuotas struct {
	calls []string
	err   error
}

func (q *fakeQuotas) Quota(_ context.Context, path string) (*QuotaInfo, error) {
	q.calls = append(q.calls, "get "+path)
	if q.err != nil {
		return nil, q.err
	}
	return &QuotaInfo{Path: path, Project: 3, Bytes: 1 << 30, Inodes: 100, UsedBytes: 1 << 20, UsedInodes: 1}, nil
}

func (q *fakeQuotas) SetQuota(_ context.Context, path string, bytes, inodes *uint64) (*QuotaInfo, error) {
	q.calls = append(q.calls, fmt.Sprintf("set %s %s %s", path, limitText(bytes), limitText(inodes)))
	if q.err != nil {
		return nil, q.err
	}
	return &QuotaInfo{Path: path, Project: 3}, nil
}

func startQuotaServer(t *testing.T, token string) (*Server, *fakeQuotas) {
	t.Helper()
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken(token))
	q := &fakeQuotas{}
	srv.SetQuotaService(q)
	return srv, q
}

// GET reads a directory's quota and PUT sets it, each with the token; a limit left out of a PUT reaches the service
// as one to leave alone.
func TestQuotaEndpoints(t *testing.T) {
	srv, q := startQuotaServer(t, "tok")
	status, body := call(t, srv, "GET", "/api/v1/quotas?path=/pvc%201", "tok", "")
	var got QuotaInfo
	if status != http.StatusOK || json.Unmarshal([]byte(body), &got) != nil || got.Path != "/pvc 1" || got.Project != 3 || got.UsedBytes != 1<<20 {
		t.Fatalf("GET: %d %s", status, body)
	}
	for _, c := range []struct{ body, want string }{
		{`{"path":"/pvc","bytes":1048576,"inodes":10}`, "set /pvc 1048576 10"},
		{`{"path":"/pvc","bytes":2097152}`, "set /pvc 2097152 unchanged"},
		{`{"path":"/pvc","inodes":0}`, "set /pvc unchanged 0"},
	} {
		status, body = call(t, srv, "PUT", "/api/v1/quotas", "tok", c.body)
		if status != http.StatusOK || q.calls[len(q.calls)-1] != c.want {
			t.Errorf("PUT %s: %d %s, service saw %v, want %q", c.body, status, body, q.calls, c.want)
		}
	}
}

// Without the token, from another origin, or with no limit, a request is refused before it reaches the service.
func TestQuotaEndpointsRefuse(t *testing.T) {
	srv, _ := startQuotaServer(t, "")
	for _, method := range []string{"GET", "PUT"} {
		if status, _ := call(t, srv, method, "/api/v1/quotas?path=/pvc", "", `{"path":"/pvc","bytes":1}`); status != http.StatusForbidden {
			t.Errorf("%s without a token set: %d, want 403", method, status)
		}
	}
	srv, q := startQuotaServer(t, "tok")
	for _, c := range []struct {
		method, body string
		want         int
	}{
		{"PUT", `{"path":"/pvc"}`, http.StatusBadRequest},
		{"PUT", `{"path":"/pvc","bytes":-1}`, http.StatusBadRequest},
		{"PUT", `{"path":"/pvc","bytes":1,"soft":2}`, http.StatusBadRequest},
		{"POST", `{"path":"/pvc","bytes":1}`, http.StatusMethodNotAllowed},
		{"DELETE", "", http.StatusMethodNotAllowed},
	} {
		if status, body := call(t, srv, c.method, "/api/v1/quotas", "tok", c.body); status != c.want {
			t.Errorf("%s %s: %d %s, want %d", c.method, c.body, status, body, c.want)
		}
	}
	if status, _ := call(t, srv, "GET", "/api/v1/quotas?path=/pvc", "wrong", ""); status != http.StatusUnauthorized {
		t.Errorf("GET with a wrong token: %d, want 401", status)
	}
	req, err := http.NewRequest("PUT", "http://"+srv.Addr()+"/api/v1/quotas", strings.NewReader(`{"path":"/pvc","bytes":1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer tok")
	req.Header.Set("Origin", "https://evil.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	closeBody(resp)
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("PUT from another origin: %d, want 403", resp.StatusCode)
	}
	if len(q.calls) != 0 {
		t.Errorf("refused requests reached the service: %v", q.calls)
	}
}

// The errors of the quota xattr map to statuses a client can act on.
func TestQuotaErrorsMapToStatus(t *testing.T) {
	srv, q := startQuotaServer(t, "tok")
	for errno, want := range map[syscall.Errno]int{
		syscall.ENODATA:   http.StatusNotFound,
		syscall.ENOENT:    http.StatusNotFound,
		syscall.ENOTEMPTY: http.StatusConflict,
		syscall.ENOTSUP:   http.StatusConflict,
		syscall.EPERM:     http.StatusForbidden,
		syscall.ENOTDIR:   http.StatusBadRequest,
		syscall.ENOSPC:    http.StatusInsufficientStorage,
	} {
		q.err = fmt.Errorf("/pvc: %w", errno)
		if status, body := call(t, srv, "PUT", "/api/v1/quotas", "tok", `{"path":"/pvc","bytes":1}`); status != want || !strings.Contains(body, errno.Error()) {
			t.Errorf("%v: %d %s, want %d", errno, status, body, want)
		}
	}
}

func TestQuotaEndpointWithoutService(t *testing.T) {
	srv := startServer(t, DefaultConfig().WithListenAddr("127.0.0.1:0").WithAuthToken("tok"))
	if status, _ := call(t, srv, "GET", "/api/v1/quotas?path=/pvc", "tok", ""); status != http.StatusServiceUnavailable {
		t.Errorf("GET with no quota service: %d, want 503", status)
	}
}

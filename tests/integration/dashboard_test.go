package integration

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func getDashboard(t *testing.T, url, token string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := writeClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer mustClose(t, resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(body)
}

// TestDashboardOnLiveMount checks /ui/ with whichever build the binary has: CI's Go jobs embed the placeholder.
func TestDashboardOnLiveMount(t *testing.T) {
	const token = "dashboard-test-token"
	m := startAPIMount(t, token, 20)
	base := "http://" + m.addr

	resp, shell := getDashboard(t, base+"/ui/snapshots", "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/ui/snapshots without a token: status %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") || !strings.Contains(csp, "frame-ancestors 'none'") {
		t.Errorf("Content-Security-Policy = %q", csp)
	}
	if !strings.Contains(shell, `id="root"`) && !strings.Contains(shell, "make dashboard build") {
		t.Errorf("/ui/snapshots is neither the dashboard nor the placeholder:\n%s", shell)
	}

	_, withToken := getDashboard(t, base+"/ui/snapshots", token)
	if withToken != shell {
		t.Error("the /ui/ shell changes when the request carries a token; it must be static")
	}
	for name, values := range resp.Header {
		if strings.Contains(strings.Join(values, " "), token) {
			t.Errorf("header %s carries the token", name)
		}
	}
	if strings.Contains(shell, token) {
		t.Error("the /ui/ shell contains the token")
	}

	if resp, _ := getDashboard(t, base+"/api/v1/status", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("/api/v1/status without a token: status %d, want 401", resp.StatusCode)
	}
	if !strings.Contains(m.log.String(), "/ui/") {
		t.Errorf("mount log never mentions /ui/:\n%s", m.log.String())
	}
}

func TestDashboardFlagOff(t *testing.T) {
	const token = "dashboard-test-token"
	m := startAPIMount(t, token, 20, "-dashboard=false")
	if resp, _ := getDashboard(t, "http://"+m.addr+"/ui/", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("/ui/ without a token: status %d, want 401", resp.StatusCode)
	}
	if resp, _ := getDashboard(t, "http://"+m.addr+"/ui/", token); resp.StatusCode != http.StatusNotFound {
		t.Errorf("/ui/ with a token: status %d, want 404", resp.StatusCode)
	}
}

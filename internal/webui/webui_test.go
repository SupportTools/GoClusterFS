package webui

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"
)

const testIndex = `<!doctype html><html><head><script type="module" src="/ui/assets/index-abc.js"></script></head><body><div id="root"></div></body></html>`

var testBuild = fstest.MapFS{
	"index.html":           {Data: []byte(testIndex)},
	"favicon.svg":          {Data: []byte(`<svg xmlns="http://www.w3.org/2000/svg"></svg>`)},
	"assets/index-abc.js":  {Data: []byte(`console.log("dashboard")`)},
	"assets/index-abc.css": {Data: []byte(`body{margin:0}`)},
}

func get(t *testing.T, h http.Handler, method, target string) *http.Response {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, target, nil))
	return rec.Result()
}

func body(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestServesBuild(t *testing.T) {
	h := newHandler(testBuild)
	tests := []struct {
		target, wantType, wantBody, wantCache string
	}{
		{"/", "text/html; charset=utf-8", testIndex, "no-cache"},
		{"/index.html", "text/html; charset=utf-8", testIndex, "no-cache"},
		{"/snapshots", "text/html; charset=utf-8", testIndex, "no-cache"},
		{"/cluster/", "text/html; charset=utf-8", testIndex, "no-cache"},
		{"/assets", "text/html; charset=utf-8", testIndex, "no-cache"},
		{"/assets/index-abc.js", "text/javascript; charset=utf-8", `console.log("dashboard")`, "public, max-age=31536000, immutable"},
		{"/assets/index-abc.css", "text/css; charset=utf-8", `body{margin:0}`, "public, max-age=31536000, immutable"},
		{"/favicon.svg", "image/svg+xml", `<svg xmlns="http://www.w3.org/2000/svg"></svg>`, "no-cache"},
		{"/../index.html", "text/html; charset=utf-8", testIndex, "no-cache"},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			resp := get(t, h, http.MethodGet, tt.target)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			if got := resp.Header.Get("Content-Type"); got != tt.wantType {
				t.Errorf("Content-Type = %q, want %q", got, tt.wantType)
			}
			if got := resp.Header.Get("Cache-Control"); got != tt.wantCache {
				t.Errorf("Cache-Control = %q, want %q", got, tt.wantCache)
			}
			if got := body(t, resp); got != tt.wantBody {
				t.Errorf("body = %q, want %q", got, tt.wantBody)
			}
		})
	}
}

func TestSecurityHeaders(t *testing.T) {
	for _, h := range []http.Handler{newHandler(testBuild), newHandler(fstest.MapFS{})} {
		for _, target := range []string{"/", "/assets/index-abc.js", "/missing.js"} {
			resp := get(t, h, http.MethodGet, target)
			if got := resp.Header.Get("Content-Security-Policy"); got != ContentSecurityPolicy {
				t.Errorf("%s: CSP = %q", target, got)
			}
			if got := resp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("%s: X-Content-Type-Options = %q", target, got)
			}
		}
	}
	for _, want := range []string{"default-src 'self'", "connect-src 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(ContentSecurityPolicy, want) {
			t.Errorf("CSP lacks %q", want)
		}
	}
}

func TestMissingFilesAre404(t *testing.T) {
	h := newHandler(testBuild)
	for _, target := range []string{"/assets/gone.js", "/assets/x", "/robots.txt"} {
		if resp := get(t, h, http.MethodGet, target); resp.StatusCode != http.StatusNotFound {
			t.Errorf("%s: status = %d, want 404", target, resp.StatusCode)
		}
	}
}

func TestOnlyGetAndHead(t *testing.T) {
	h := newHandler(testBuild)
	if resp := get(t, h, http.MethodHead, "/"); resp.StatusCode != http.StatusOK {
		t.Errorf("HEAD status = %d, want 200", resp.StatusCode)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		resp := get(t, h, method, "/")
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s status = %d, want 405", method, resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); got != "GET, HEAD" {
			t.Errorf("%s Allow = %q", method, got)
		}
	}
}

func TestPlaceholderWhenNotBuilt(t *testing.T) {
	h := newHandler(fstest.MapFS{})
	for _, target := range []string{"/", "/snapshots", "/assets/index-abc.js"} {
		resp := get(t, h, http.MethodGet, target)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", target, resp.StatusCode)
		}
		if got := body(t, resp); !strings.Contains(got, "make dashboard") {
			t.Errorf("%s: body is not the placeholder: %q", target, got)
		}
	}
}

var assetRef = regexp.MustCompile(`(?:src|href)="/ui/([^"]+)"`)

// TestEmbeddedBuild checks the real build when `make dashboard` produced one.
func TestEmbeddedBuild(t *testing.T) {
	if !Built() {
		t.Skip("dashboard not built; run make dashboard")
	}
	index, err := fs.ReadFile(dist, "index.html")
	if err != nil {
		t.Fatal(err)
	}
	refs := assetRef.FindAllStringSubmatch(string(index), -1)
	if len(refs) == 0 {
		t.Fatalf("index.html references nothing under /ui/; is Vite's base set to /ui/?\n%s", index)
	}
	h := Handler()
	for _, ref := range refs {
		resp := get(t, h, http.MethodGet, "/"+ref[1])
		if resp.StatusCode != http.StatusOK || strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
			t.Errorf("/ui/%s: status %d, type %q", ref[1], resp.StatusCode, resp.Header.Get("Content-Type"))
		}
	}
	err = fs.WalkDir(dist, ".", func(p string, d fs.DirEntry, err error) error {
		if err == nil && strings.HasSuffix(p, ".map") {
			t.Errorf("source map %s is embedded", p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

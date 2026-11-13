// Package webui serves the web/dashboard build that `make dashboard` writes to build/dist.
package webui

import (
	"bytes"
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"
)

//go:embed all:build
var build embed.FS

//go:embed placeholder.html
var placeholder []byte

// ContentSecurityPolicy allows data: images for the SVG icons Tailwind's form styles inline.
const ContentSecurityPolicy = "default-src 'self'; connect-src 'self'; img-src 'self' data:; " +
	"object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"

var dist = mustSub(build, "build/dist")

// contentTypes overrides /etc/mime.types, which differs between hosts.
var contentTypes = map[string]string{
	".html": "text/html; charset=utf-8",
	".js":   "text/javascript; charset=utf-8",
	".css":  "text/css; charset=utf-8",
	".svg":  "image/svg+xml",
}

func mustSub(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

// Built reports whether the binary holds a real dashboard build.
func Built() bool { return hasIndex(dist) }

func hasIndex(fsys fs.FS) bool {
	_, err := fs.Stat(fsys, "index.html")
	return err == nil
}

// Handler expects paths relative to the dashboard root and answers unknown routes with index.html.
func Handler() http.Handler { return newHandler(dist) }

func newHandler(files fs.FS) http.Handler {
	built := hasIndex(files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		h := w.Header()
		h.Set("Content-Security-Policy", ContentSecurityPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")

		if !built {
			serve(w, r, "index.html", placeholder)
			return
		}
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		data, err := fs.ReadFile(files, name)
		if err != nil {
			if strings.HasPrefix(name, "assets/") || path.Ext(name) != "" {
				http.NotFound(w, r)
				return
			}
			name = "index.html"
			if data, err = fs.ReadFile(files, name); err != nil {
				http.Error(w, "dashboard index missing", http.StatusInternalServerError)
				return
			}
		}
		serve(w, r, name, data)
	})
}

// serve caches Vite's content-hashed assets for good and makes browsers revalidate the rest.
func serve(w http.ResponseWriter, r *http.Request, name string, data []byte) {
	if strings.HasPrefix(name, "assets/") {
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	} else {
		w.Header().Set("Cache-Control", "no-cache")
	}
	if ct, ok := contentTypes[path.Ext(name)]; ok {
		w.Header().Set("Content-Type", ct)
	}
	http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
}

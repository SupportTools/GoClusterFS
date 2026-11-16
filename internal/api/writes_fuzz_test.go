package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

const fuzzToken = "fuzz-token-0123456789"

// fuzzCall is one call the API made into the file or admin service.
type fuzzCall struct {
	op, path, name, dest string
}

type fuzzServices struct{ calls []fuzzCall }

func (s *fuzzServices) ListSnapshots(_ context.Context, path string) ([]SnapshotInfo, error) {
	return []SnapshotInfo{{ID: 1, Name: "s", Inode: 3}}, nil
}
func (s *fuzzServices) CreateSnapshot(_ context.Context, path, name string) error {
	s.calls = append(s.calls, fuzzCall{"snapshot.create", path, name, ""})
	return nil
}
func (s *fuzzServices) DeleteSnapshot(_ context.Context, path, name string) error {
	s.calls = append(s.calls, fuzzCall{"snapshot.delete", path, name, ""})
	return nil
}
func (s *fuzzServices) CloneSnapshot(_ context.Context, path, name, dest string) error {
	s.calls = append(s.calls, fuzzCall{"snapshot.clone", path, name, dest})
	return nil
}
func (s *fuzzServices) CloneFile(_ context.Context, path, dest string) error {
	s.calls = append(s.calls, fuzzCall{"clone", path, "", dest})
	return nil
}
func (s *fuzzServices) FlushJournal() (string, error) {
	s.calls = append(s.calls, fuzzCall{op: "admin.flush-journal"})
	return "ok", nil
}
func (s *fuzzServices) ForceTrim() (string, error) {
	s.calls = append(s.calls, fuzzCall{op: "admin.force-trim"})
	return "ok", nil
}
func (s *fuzzServices) ClearCache() (string, error) {
	s.calls = append(s.calls, fuzzCall{op: "admin.clear-cache"})
	return "ok", nil
}

var fuzzMethods = []string{http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodPut, http.MethodPatch, http.MethodOptions, http.MethodHead}

// FuzzWriteRequests sends arbitrary requests to the API. A write must reach
// the file or admin service only with the token, from the same origin, and
// with exactly the fields of a valid body.
func FuzzWriteRequests(f *testing.F) {
	for _, c := range writeCalls {
		for i, m := range fuzzMethods {
			if m == c.method {
				f.Add(true, uint8(i), c.path, true, "", "", []byte(c.body))
				f.Add(true, uint8(i), c.path, false, "wrong", "http://evil.example", []byte(c.body))
			}
		}
	}
	f.Add(true, uint8(1), "/api/v1/admin/flush-journal", true, "", "http://127.0.0.1:8080", []byte{})
	f.Add(true, uint8(1), "/api/v1/snapshots", true, "", "", []byte(`{"path":"/f","name":"s","extra":1}`))
	f.Add(true, uint8(1), "/api/v1/clones", true, "", "", []byte(`{"path":"/f","dest":"/c"}{"path":"/g"}`))
	f.Add(true, uint8(2), "/api/v1/snapshots/a%2Fb?path=/f", true, "", "", []byte{})
	f.Add(false, uint8(1), "/api/v1/clones", true, "", "", []byte(`{"path":"/f","dest":"/c"}`))
	f.Add(true, uint8(1), "/api/v1/snapshots", true, "", "", bytes.Repeat([]byte(" "), maxWriteBody+1))

	log.SetOutput(io.Discard)
	f.Cleanup(func() { log.SetOutput(os.Stderr) })
	f.Fuzz(func(t *testing.T, writesOn bool, method uint8, target string, goodToken bool, token, origin string, body []byte) {
		cfg := DefaultConfig()
		if writesOn {
			cfg = cfg.WithAuthToken(fuzzToken)
		}
		srv, err := NewServer(cfg)
		if err != nil {
			t.Fatal(err)
		}
		services := &fuzzServices{}
		srv.SetFileService(services)
		srv.SetAdminService(services)

		if !strings.HasPrefix(target, "/") {
			target = "/" + target
		}
		req, err := http.NewRequest(fuzzMethods[int(method)%len(fuzzMethods)], "http://127.0.0.1:8080"+target, bytes.NewReader(body))
		if err != nil {
			return
		}
		if goodToken {
			token = fuzzToken
		}
		auth := "Bearer " + token
		req.Header.Set("Authorization", auth)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		srv.server.Handler.ServeHTTP(rec, req)

		if rec.Code < 100 || rec.Code > 599 {
			t.Fatalf("status %d", rec.Code)
		}
		redirect := rec.Code >= 300 && rec.Code < 400
		if strings.HasPrefix(req.URL.Path, "/api/v1/") && !redirect && req.Method != http.MethodHead && rec.Body.Len() > 0 && !json.Valid(rec.Body.Bytes()) {
			t.Fatalf("%s %s answered %d with a body that is not JSON: %q", req.Method, target, rec.Code, rec.Body.String())
		}
		if len(services.calls) == 0 {
			return
		}
		call := services.calls[0]
		switch {
		case len(services.calls) > 1:
			t.Fatalf("one request made %d service calls: %+v", len(services.calls), services.calls)
		case !writesOn || auth != "Bearer "+fuzzToken:
			t.Fatalf("%s reached the service without the token (writes on %v)", call.op, writesOn)
		case origin != "" && !sameOrigin(origin, req.Host):
			t.Fatalf("%s reached the service from origin %q for host %q", call.op, origin, req.Host)
		case req.Method != http.MethodPost && req.Method != http.MethodDelete:
			t.Fatalf("%s reached the service with method %s", call.op, req.Method)
		case len(body) > maxWriteBody:
			t.Fatalf("%s reached the service with a %d-byte body", call.op, len(body))
		}
		checkCallArgs(t, call, req, body)
	})
}

// checkCallArgs checks that a body call got exactly the fields of the one
// strict JSON object the body holds, and a path call those of the URL.
func checkCallArgs(t *testing.T, call fuzzCall, req *http.Request, body []byte) {
	strict := func(v any) bool {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		var trailing struct{}
		return dec.Decode(v) == nil && dec.Decode(&trailing) != nil
	}
	switch call.op {
	case "snapshot.create":
		var want SnapshotRequest
		if !strict(&want) || want.Path != call.path || want.Name != call.name {
			t.Fatalf("snapshot.create got %+v from body %q", call, body)
		}
	case "snapshot.clone", "clone":
		var want CloneRequest
		if !strict(&want) || want.Path != call.path || want.Dest != call.dest {
			t.Fatalf("%s got %+v from body %q", call.op, call, body)
		}
	case "snapshot.delete":
		if call.path != req.URL.Query().Get("path") || call.name == "" {
			t.Fatalf("snapshot.delete got %+v from %s", call, req.URL)
		}
	default:
		if "admin."+strings.TrimPrefix(req.URL.Path, "/api/v1/admin/") != call.op {
			t.Fatalf("%s reached from path %s", call.op, req.URL.Path)
		}
	}
}

package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// FileService runs snapshot and clone requests on files in the mounted filesystem.
// Paths are absolute within the mount, such as /dir/file.
type FileService interface {
	ListSnapshots(ctx context.Context, path string) ([]SnapshotInfo, error)
	CreateSnapshot(ctx context.Context, path, name string) error
	DeleteSnapshot(ctx context.Context, path, name string) error
	CloneSnapshot(ctx context.Context, path, name, dest string) error
	CloneFile(ctx context.Context, path, dest string) error
}

// AdminService runs maintenance on this node. Each method returns a short
// description of what it did.
type AdminService interface {
	FlushJournal() (string, error)
	ForceTrim() (string, error)
	ClearCache() (string, error)
}

var (
	// ErrInvalidPath is returned for a path or name the API refuses.
	ErrInvalidPath = errors.New("invalid path")

	// ErrDisabled is returned when the feature is turned off by a mount flag.
	ErrDisabled = errors.New("disabled")

	// ErrNotMounted is returned when the filesystem is no longer mounted.
	ErrNotMounted = errors.New("not mounted")
)

// SnapshotInfo describes one snapshot in the registry.
type SnapshotInfo struct {
	ID        uint64 `json:"id"`
	Name      string `json:"name"`
	Inode     uint32 `json:"inode"`
	State     string `json:"state"`
	SizeBytes uint64 `json:"size_bytes"`
	Blocks    uint64 `json:"blocks"`
	CreatedAt string `json:"created_at,omitempty"`
	// Orphan marks a snapshot whose file is gone, or that cannot prove which file it was taken of.
	Orphan bool `json:"orphan,omitempty"`
}

// SnapshotListResponse is returned by GET /api/v1/snapshots.
type SnapshotListResponse struct {
	Snapshots []SnapshotInfo `json:"snapshots"`
	Total     int            `json:"total"`
}

// SnapshotRequest is sent to POST /api/v1/snapshots.
type SnapshotRequest struct {
	Path string `json:"path"`
	Name string `json:"name"`
}

// CloneRequest is sent to POST /api/v1/clones and POST /api/v1/snapshots/{name}/clone.
type CloneRequest struct {
	Path string `json:"path"`
	Dest string `json:"dest"`
}

// WriteResponse is returned by successful snapshot and clone requests.
type WriteResponse struct {
	Action string `json:"action"`
	Path   string `json:"path"`
	Name   string `json:"name,omitempty"`
	Dest   string `json:"dest,omitempty"`
}

// AdminResponse is returned by POST /api/v1/admin/*.
type AdminResponse struct {
	Operation string `json:"operation"`
	Message   string `json:"message"`
}

const (
	maxWriteBody = 64 << 10
	// A snapshot or clone of a large file can outlast the server's write timeout.
	writeOpTimeout = 10 * time.Minute
)

// SetFileService sets the service behind the snapshot and clone endpoints.
func (s *Server) SetFileService(fs FileService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fileService = fs
}

// SetAdminService sets the service behind the admin endpoints.
func (s *Server) SetAdminService(as AdminService) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.adminService = as
}

// WritesEnabled reports whether write and snapshot endpoints accept requests. They
// need an auth token even on loopback: the Host check stops browsers, not local processes.
func (s *Server) WritesEnabled() bool {
	return s.config.AuthToken != ""
}

func (s *Server) services() (FileService, AdminService) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fileService, s.adminService
}

// requireToken refuses requests when no token is set. Snapshot data needs one
// even on loopback, like writes: the API reads it as the mount's user.
func (s *Server) requireToken(w http.ResponseWriter) bool {
	if !s.WritesEnabled() {
		writeError(w, http.StatusForbidden, "snapshot and admin endpoints are disabled", "start goclusterfs-mount with -api-token-file or GOCLUSTERFS_API_TOKEN")
		return false
	}
	return true
}

// beginWrite refuses writes without a token, from another origin, or once
// the server is stopping.
func (s *Server) beginWrite(w http.ResponseWriter, r *http.Request) bool {
	if !s.requireToken(w) {
		return false
	}
	if origin := r.Header.Get("Origin"); origin != "" && !sameOrigin(origin, r.Host) {
		writeError(w, http.StatusForbidden, "cross-origin writes are refused", "")
		return false
	}
	s.mu.RLock()
	stopping := s.stopping
	s.mu.RUnlock()
	if stopping {
		writeError(w, http.StatusServiceUnavailable, "server is shutting down", "")
		return false
	}
	return true
}

func sameOrigin(origin, host string) bool {
	u, err := url.Parse(origin)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Host, host)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxWriteBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large", fmt.Sprintf("at most %d bytes", maxWriteBody))
			return false
		}
		writeError(w, http.StatusBadRequest, "invalid request body", err.Error())
		return false
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid request body", "expected a single JSON object")
		return false
	}
	return true
}

func logWrite(r *http.Request, action, detail string, err error) {
	result := "ok"
	if err != nil {
		result = err.Error()
	}
	log.Printf("[INFO] api write: client=%s action=%s %s result=%q", getClientIP(r), action, detail, result)
}

// handleSnapshots serves /api/v1/snapshots, /api/v1/snapshots/{name} and /api/v1/snapshots/{name}/clone.
func (s *Server) handleSnapshots(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.EscapedPath(), "/api/v1/snapshots")
	rest = strings.TrimPrefix(rest, "/")
	escapedName, sub, _ := strings.Cut(rest, "/")
	name, err := url.PathUnescape(escapedName)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid snapshot name", err.Error())
		return
	}

	switch {
	case name == "" && r.Method == http.MethodGet:
		s.listSnapshots(w, r)
	case name == "" && r.Method == http.MethodPost:
		s.createSnapshot(w, r)
	case name != "" && sub == "" && r.Method == http.MethodDelete:
		s.deleteSnapshot(w, r, name)
	case name != "" && sub == "clone" && r.Method == http.MethodPost:
		s.cloneSnapshot(w, r, name)
	case name == "" || sub == "" || sub == "clone":
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "")
	default:
		writeError(w, http.StatusNotFound, "snapshot endpoint not found", "")
	}
}

func (s *Server) fileServiceOrError(w http.ResponseWriter) FileService {
	fs, _ := s.services()
	if fs == nil {
		writeError(w, http.StatusServiceUnavailable, "snapshots not available", "no file service configured")
	}
	return fs
}

func (s *Server) listSnapshots(w http.ResponseWriter, r *http.Request) {
	if !s.requireToken(w) {
		return
	}
	fs := s.fileServiceOrError(w)
	if fs == nil {
		return
	}
	snaps, err := fs.ListSnapshots(r.Context(), r.URL.Query().Get("path"))
	if err != nil {
		writeOpError(w, err)
		return
	}
	if snaps == nil {
		snaps = []SnapshotInfo{}
	}
	writeJSON(w, http.StatusOK, SnapshotListResponse{Snapshots: snaps, Total: len(snaps)})
}

func (s *Server) createSnapshot(w http.ResponseWriter, r *http.Request) {
	if !s.beginWrite(w, r) {
		return
	}
	fs := s.fileServiceOrError(w)
	if fs == nil {
		return
	}
	var req SnapshotRequest
	if !decodeBody(w, r, &req) {
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(writeOpTimeout))
	err := fs.CreateSnapshot(r.Context(), req.Path, req.Name)
	logWrite(r, "snapshot.create", fmt.Sprintf("path=%q name=%q", req.Path, req.Name), err)
	if err != nil {
		writeOpError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, WriteResponse{Action: "snapshot.create", Path: req.Path, Name: req.Name})
}

func (s *Server) deleteSnapshot(w http.ResponseWriter, r *http.Request, name string) {
	if !s.beginWrite(w, r) {
		return
	}
	fs := s.fileServiceOrError(w)
	if fs == nil {
		return
	}
	path := r.URL.Query().Get("path")
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(writeOpTimeout))
	err := fs.DeleteSnapshot(r.Context(), path, name)
	logWrite(r, "snapshot.delete", fmt.Sprintf("path=%q name=%q", path, name), err)
	if err != nil {
		writeOpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, WriteResponse{Action: "snapshot.delete", Path: path, Name: name})
}

func (s *Server) cloneSnapshot(w http.ResponseWriter, r *http.Request, name string) {
	if !s.beginWrite(w, r) {
		return
	}
	fs := s.fileServiceOrError(w)
	if fs == nil {
		return
	}
	var req CloneRequest
	if !decodeBody(w, r, &req) {
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(writeOpTimeout))
	err := fs.CloneSnapshot(r.Context(), req.Path, name, req.Dest)
	logWrite(r, "snapshot.clone", fmt.Sprintf("path=%q name=%q dest=%q", req.Path, name, req.Dest), err)
	if err != nil {
		writeOpError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, WriteResponse{Action: "snapshot.clone", Path: req.Path, Name: name, Dest: req.Dest})
}

// handleClones serves POST /api/v1/clones.
func (s *Server) handleClones(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/v1/clones" {
		writeError(w, http.StatusNotFound, "clone endpoint not found", "")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use POST")
		return
	}
	if !s.beginWrite(w, r) {
		return
	}
	fs := s.fileServiceOrError(w)
	if fs == nil {
		return
	}
	var req CloneRequest
	if !decodeBody(w, r, &req) {
		return
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(writeOpTimeout))
	err := fs.CloneFile(r.Context(), req.Path, req.Dest)
	logWrite(r, "clone", fmt.Sprintf("path=%q dest=%q", req.Path, req.Dest), err)
	if err != nil {
		writeOpError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, WriteResponse{Action: "clone", Path: req.Path, Dest: req.Dest})
}

// handleAdmin serves POST /api/v1/admin/{flush-journal,force-trim,clear-cache}.
func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	op := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/")
	if op != "flush-journal" && op != "force-trim" && op != "clear-cache" {
		writeError(w, http.StatusNotFound, "admin endpoint not found", "valid endpoints: flush-journal, force-trim, clear-cache")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed", "use POST")
		return
	}
	if !s.beginWrite(w, r) {
		return
	}
	_, as := s.services()
	if as == nil {
		writeError(w, http.StatusServiceUnavailable, "admin not available", "no admin service configured")
		return
	}

	var msg string
	var err error
	switch op {
	case "flush-journal":
		msg, err = as.FlushJournal()
	case "force-trim":
		msg, err = as.ForceTrim()
	case "clear-cache":
		msg, err = as.ClearCache()
	}
	logWrite(r, "admin."+op, "", err)
	if err != nil {
		writeOpError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, AdminResponse{Operation: op, Message: msg})
}

// writeOpError maps a service error to an HTTP status.
func writeOpError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var errno syscall.Errno
	switch {
	case errors.Is(err, ErrInvalidPath):
		status = http.StatusBadRequest
	case errors.Is(err, ErrDisabled):
		status = http.StatusConflict
	case errors.Is(err, ErrNotMounted):
		status = http.StatusServiceUnavailable
	case errors.As(err, &errno):
		switch errno {
		case syscall.ENOENT:
			status = http.StatusNotFound
		case syscall.EEXIST, syscall.ENOTSUP, syscall.EBUSY, syscall.EAGAIN:
			status = http.StatusConflict
		case syscall.EINVAL, syscall.ENAMETOOLONG, syscall.EISDIR, syscall.ENOTDIR, syscall.ELOOP, syscall.EXDEV:
			status = http.StatusBadRequest
		case syscall.ENOSPC:
			status = http.StatusInsufficientStorage
		case syscall.EACCES, syscall.EPERM:
			status = http.StatusForbidden
		case syscall.ENOSYS:
			status = http.StatusNotImplemented
		}
	}
	writeError(w, status, http.StatusText(status), err.Error())
}

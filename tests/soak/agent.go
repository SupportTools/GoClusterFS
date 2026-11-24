//go:build linux

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// request is one operation the driver asks an agent to run. Paths are relative to the mount.
type request struct {
	ID    uint64 `json:"id"`
	Op    string `json:"op"`
	Path  string `json:"path,omitempty"`
	Path2 string `json:"path2,omitempty"`
	Attr  string `json:"attr,omitempty"`
	Value string `json:"value,omitempty"`
	Off   int64  `json:"off,omitempty"`
	Len   int64  `json:"len,omitempty"`
	Seed  uint64 `json:"seed,omitempty"`
}

type response struct {
	ID    uint64   `json:"id"`
	Errno int      `json:"errno"`
	Err   string   `json:"err,omitempty"`
	Size  int64    `json:"size,omitempty"`
	Sum   string   `json:"sum,omitempty"`
	Names []string `json:"names,omitempty"`
	Text  string   `json:"text,omitempty"`
	Data  []byte   `json:"data,omitempty"`
}

// genData returns n bytes that depend only on seed, so the driver and the agents produce the same data.
func genData(seed uint64, n int64) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	for i := int64(0); i < n; i += 8 {
		v := r.Uint64()
		for j := int64(0); j < 8 && i+j < n; j++ {
			b[i+j] = byte(v >> (8 * j))
		}
	}
	return b
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}

// runAgent serves requests from stdin until it closes, running each in its own goroutine.
func runAgent(root string, in io.Reader, out io.Writer) error {
	dec := json.NewDecoder(in)
	enc := json.NewEncoder(out)
	var mu sync.Mutex
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := serve(root, req)
			resp.ID = req.ID
			mu.Lock()
			defer mu.Unlock()
			if err := enc.Encode(resp); err != nil {
				fmt.Fprintf(os.Stderr, "agent: reply %d: %v\n", req.ID, err)
			}
		}()
	}
}

func serve(root string, req request) response {
	var resp response
	p := filepath.Join(root, req.Path)
	var err error
	switch req.Op {
	case "ping":
	case "create":
		var f *os.File
		if f, err = os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); err == nil {
			err = f.Close()
		}
	case "write":
		resp.Size, err = writeSynced(p, req.Off, genData(req.Seed, req.Len))
	case "truncate":
		err = unix.Truncate(p, req.Len)
	case "unlink":
		err = unix.Unlink(p)
	case "mkdir":
		err = unix.Mkdir(p, 0o755)
	case "rmdir":
		err = unix.Rmdir(p)
	case "rename":
		err = unix.Rename(p, filepath.Join(root, req.Path2))
	case "symlink":
		err = unix.Symlink(req.Path2, p)
	case "link":
		err = unix.Link(p, filepath.Join(root, req.Path2))
	case "setxattr":
		err = unix.Setxattr(p, req.Attr, []byte(req.Value), 0)
	case "getxattr":
		buf := make([]byte, 64<<10)
		var n int
		if n, err = unix.Getxattr(p, req.Attr, buf); err == nil {
			resp.Text = string(buf[:n])
		}
	case "statfs":
		var st unix.Statfs_t
		if err = unix.Statfs(p, &st); err == nil {
			resp.Text = fmt.Sprintf("%d %d", int64(st.Blocks)*st.Frsize, st.Files)
		}
	case "read":
		var data []byte
		if data, err = os.ReadFile(p); err == nil {
			resp.Size, resp.Sum = int64(len(data)), sum(data)
		}
	case "readall":
		if resp.Data, err = os.ReadFile(p); err == nil {
			resp.Size, resp.Sum = int64(len(resp.Data)), sum(resp.Data)
		}
	case "apidelete":
		_, err = callAPI(http.MethodDelete, "/"+url.PathEscape(req.Value))
	case "apilist":
		resp.Names, err = listSnapshotsAPI()
	case "list":
		var entries []os.DirEntry
		if entries, err = os.ReadDir(p); err == nil {
			for _, e := range entries {
				resp.Names = append(resp.Names, entryName(p, e))
			}
		}
	default:
		err = fmt.Errorf("unknown op %q", req.Op)
	}
	if err != nil {
		resp.Err = err.Error()
		resp.Errno = -1
		var errno syscall.Errno
		if errors.As(err, &errno) {
			resp.Errno = int(errno)
		}
	}
	return resp
}

// entryName describes a directory entry as "d:name", "l:name:target" or "f:name:size".
func entryName(dir string, e os.DirEntry) string {
	if e.IsDir() {
		return "d:" + e.Name()
	}
	if e.Type()&os.ModeSymlink != 0 {
		target, err := os.Readlink(filepath.Join(dir, e.Name()))
		if err != nil {
			return "l:" + e.Name() + ":?"
		}
		return "l:" + e.Name() + ":" + target
	}
	var st unix.Stat_t
	if err := unix.Lstat(filepath.Join(dir, e.Name()), &st); err != nil {
		return "f:" + e.Name() + ":?"
	}
	return fmt.Sprintf("f:%s:%d", e.Name(), st.Size)
}

// writeSynced writes data at off and syncs the file. It returns how much a failed write wrote, synced too, since a
// write that passes a quota limit part way keeps its first bytes.
func writeSynced(p string, off int64, data []byte) (int64, error) {
	f, err := os.OpenFile(p, os.O_WRONLY, 0)
	if err != nil {
		return 0, err
	}
	n, werr := f.WriteAt(data, off)
	serr := f.Sync()
	cerr := f.Close()
	if werr != nil {
		return int64(n), werr
	}
	return 0, errors.Join(serr, cerr)
}

// apiToken matches GOCLUSTERFS_API_TOKEN in cluster.sh, which turns on the API's snapshot writes.
const apiToken = "soak-api-token"

var apiClient = &http.Client{Timeout: 5 * time.Minute}

// callAPI sends a request to the node's snapshot API and turns the HTTP status into an errno.
func callAPI(method, rest string) ([]byte, error) {
	req, err := http.NewRequest(method, "http://127.0.0.1:8080/api/v1/snapshots"+rest, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	resp, err := apiClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusOK:
		return body, nil
	case http.StatusNotFound:
		return nil, fmt.Errorf("%s: %w", body, syscall.ENOENT)
	case http.StatusConflict:
		return nil, fmt.Errorf("%s: %w", body, syscall.EBUSY)
	}
	return nil, fmt.Errorf("HTTP %d: %s: %w", resp.StatusCode, body, syscall.EIO)
}

// listSnapshotsAPI returns every snapshot on the volume as "name:state".
func listSnapshotsAPI() ([]string, error) {
	body, err := callAPI(http.MethodGet, "")
	if err != nil {
		return nil, err
	}
	var list struct {
		Snapshots []struct {
			Name  string `json:"name"`
			State string `json:"state"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(list.Snapshots))
	for _, s := range list.Snapshots {
		names = append(names, s.Name+":"+s.State)
	}
	return names, nil
}

package integration

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type apiMount struct {
	*MountedFS
	addr  string
	token string
	log   *lockedBuffer
}

func writeTokenFile(t *testing.T, token string, mode os.FileMode) string {
	t.Helper()
	p := t.TempDir() + "/token"
	if err := os.WriteFile(p, []byte(token+"\n"), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatal(err)
	}
	return p
}

func startAPIMount(t *testing.T, token string, sizeMB int, extraArgs ...string) *apiMount {
	t.Helper()
	m := &apiMount{MountedFS: NewMountedFS(t, sizeMB), addr: freeLoopbackAddr(t), token: token, log: &lockedBuffer{}}
	m.Log = m.log
	m.ExtraArgs = append([]string{"-format", "-api-addr", m.addr}, extraArgs...)
	if token != "" {
		m.ExtraArgs = append(m.ExtraArgs, "-api-token-file", writeTokenFile(t, token, 0o600))
	}
	if err := m.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if resp, err := http.Get("http://" + m.addr + "/health"); err == nil {
			mustClose(t, resp.Body)
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("API never came up on %s", m.addr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

var writeClient = &http.Client{Timeout: 60 * time.Second}

func (m *apiMount) call(method, path string, body any) (int, string, error) {
	var r io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, "", err
		}
		r = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, "http://"+m.addr+path, r)
	if err != nil {
		return 0, "", err
	}
	if m.token != "" {
		req.Header.Set("Authorization", "Bearer "+m.token)
	}
	resp, err := writeClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(data), nil
}

func (m *apiMount) mustCall(t *testing.T, want int, method, path string, body any) string {
	t.Helper()
	status, out, err := m.call(method, path, body)
	if err != nil || status != want {
		t.Fatalf("%s %s: status %d err %v, want %d\n%s", method, path, status, err, want, out)
	}
	return out
}

func snapshotNames(t *testing.T, listJSON string) []string {
	t.Helper()
	var resp struct {
		Snapshots []struct {
			Name string `json:"name"`
		} `json:"snapshots"`
	}
	if err := json.Unmarshal([]byte(listJSON), &resp); err != nil {
		t.Fatalf("decode snapshot list: %v\n%s", err, listJSON)
	}
	var names []string
	for _, s := range resp.Snapshots {
		names = append(names, s.Name)
	}
	return names
}

func TestAPISnapshotLifecycle(t *testing.T) {
	m := startAPIMount(t, "secret", 50)
	before, after := bytes.Repeat([]byte("A"), 3<<20+17), bytes.Repeat([]byte("B"), 2<<20+5)
	if err := m.WriteFile("data", before); err != nil {
		t.Fatal(err)
	}

	m.mustCall(t, http.StatusCreated, "POST", "/api/v1/snapshots", map[string]string{"path": "/data", "name": "s1"})
	if err := m.WriteFile("data", after); err != nil {
		t.Fatal(err)
	}

	attrs := make([]byte, 4096)
	n, err := unix.Getxattr(m.Path("data"), "user.snapshots", attrs)
	if err != nil || !strings.Contains(string(attrs[:n]), "s1:") {
		t.Fatalf("getfattr user.snapshots after an API snapshot: %q, %v", attrs[:n], err)
	}
	if names := snapshotNames(t, m.mustCall(t, http.StatusOK, "GET", "/api/v1/snapshots?path=/data", nil)); len(names) != 1 || names[0] != "s1" {
		t.Fatalf("snapshots of /data = %v, want [s1]", names)
	}

	m.mustCall(t, http.StatusCreated, "POST", "/api/v1/snapshots/s1/clone", map[string]string{"path": "/data", "dest": "/restored"})
	if got, err := m.ReadFile("restored"); err != nil || !bytes.Equal(got, before) {
		t.Fatalf("clone of snapshot s1: %d bytes, err %v; want the %d bytes from before the overwrite", len(got), err, len(before))
	}
	m.mustCall(t, http.StatusCreated, "POST", "/api/v1/clones", map[string]string{"path": "/data", "dest": "/copy"})
	if got, err := m.ReadFile("copy"); err != nil || !bytes.Equal(got, after) {
		t.Fatalf("clone of /data: %d bytes, err %v; want %d", len(got), err, len(after))
	}

	for _, tc := range []struct {
		method, path string
		body         any
		want         int
	}{
		{"POST", "/api/v1/snapshots", map[string]string{"path": "/data", "name": "s1"}, http.StatusConflict},
		{"POST", "/api/v1/snapshots", map[string]string{"path": "/missing", "name": "s2"}, http.StatusNotFound},
		{"POST", "/api/v1/snapshots", map[string]string{"path": "/../etc/passwd", "name": "s2"}, http.StatusBadRequest},
		{"POST", "/api/v1/snapshots", map[string]string{"path": "/data", "name": "a:b"}, http.StatusBadRequest},
		{"POST", "/api/v1/clones", map[string]string{"path": "/data", "dest": "/sub/dir"}, http.StatusBadRequest},
		{"DELETE", "/api/v1/snapshots/nope?path=/data", nil, http.StatusNotFound},
	} {
		if status, out, err := m.call(tc.method, tc.path, tc.body); err != nil || status != tc.want {
			t.Errorf("%s %s %v: status %d err %v, want %d\n%s", tc.method, tc.path, tc.body, status, err, tc.want, out)
		}
	}
	noToken := &apiMount{MountedFS: m.MountedFS, addr: m.addr}
	if status, _, _ := noToken.call("POST", "/api/v1/snapshots", map[string]string{"path": "/data", "name": "s3"}); status != http.StatusUnauthorized {
		t.Errorf("snapshot without a token: status %d, want 401", status)
	}

	m.mustCall(t, http.StatusOK, "DELETE", "/api/v1/snapshots/s1?path=/data", nil)
	// The snapshot worker drops the snapshot after DELETE returns; until then it lists as deleting.
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		names := snapshotNames(t, m.mustCall(t, http.StatusOK, "GET", "/api/v1/snapshots", nil))
		if len(names) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshots 15 s after delete = %v, want none", names)
		}
	}
	if !strings.Contains(m.log.String(), "api write: client=127.0.0.1 action=snapshot.create") {
		t.Error("mount did not log the snapshot write")
	}
}

func TestAPIWritesOffWithoutToken(t *testing.T) {
	m := startAPIMount(t, "", 20)
	if err := m.WriteFile("data", []byte("x")); err != nil {
		t.Fatal(err)
	}
	status, out, err := m.call("POST", "/api/v1/snapshots", map[string]string{"path": "/data", "name": "s1"})
	if err != nil || status != http.StatusForbidden || !strings.Contains(out, "-api-token") {
		t.Fatalf("snapshot without -api-token: status %d err %v\n%s", status, err, out)
	}
	m.mustCall(t, http.StatusForbidden, "GET", "/api/v1/snapshots", nil)
	if !strings.Contains(m.log.String(), "endpoints are off; set -api-token") {
		t.Error("mount did not log that writes are off")
	}
}

const chunkSize = 4096

// chunked fills size bytes with 4 KB chunks that each name their file, version and position.
func chunked(file, version, size int) []byte {
	buf := make([]byte, size)
	for off := 0; off < size; off += chunkSize {
		c := buf[off:min(off+chunkSize, size)]
		binary.LittleEndian.PutUint32(c[0:], uint32(file))
		binary.LittleEndian.PutUint32(c[4:], uint32(version))
		binary.LittleEndian.PutUint32(c[8:], uint32(off/chunkSize))
		for i := 16; i < len(c); i++ {
			c[i] = byte(file*31 + version*7 + off/chunkSize + i)
		}
	}
	return buf
}

// checkChunked reports a chunk that is not whole or not in its place. Chunks may come from different versions.
func checkChunked(data []byte, file, size int) error {
	if len(data) != size {
		return fmt.Errorf("size %d, want %d", len(data), size)
	}
	for off := 0; off < size; off += chunkSize {
		c := data[off:min(off+chunkSize, size)]
		version := int(binary.LittleEndian.Uint32(c[4:]))
		want := chunked(file, version, size)[off : off+len(c)]
		if !bytes.Equal(c, want) {
			return fmt.Errorf("chunk %d (version %d) is torn or misplaced", off/chunkSize, version)
		}
	}
	return nil
}

// TestAPIWritesDuringFileIO runs snapshot, clone and delete requests while other goroutines write and read the same files.
func TestAPIWritesDuringFileIO(t *testing.T) {
	m := startAPIMount(t, "secret", 120)
	const files, size = 3, 3<<20 + 3*chunkSize
	for i := 0; i < files; i++ {
		if err := m.WriteFile(fmt.Sprintf("f%d", i), chunked(i, 0, size)); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var workers sync.WaitGroup
	var lastVersion [files]atomic.Int64
	for i := 0; i < files; i++ {
		workers.Add(2)
		go func() {
			defer workers.Done()
			f, err := os.OpenFile(m.Path(fmt.Sprintf("f%d", i)), os.O_RDWR, 0)
			if err != nil {
				t.Errorf("open f%d: %v", i, err)
				return
			}
			defer mustClose(t, f)
			for v := 1; ; v++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := f.WriteAt(chunked(i, v, size), 0); err != nil {
					t.Errorf("write f%d: %v", i, err)
					return
				}
				lastVersion[i].Store(int64(v))
			}
		}()
		go func() {
			defer workers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if data, err := m.ReadFile(fmt.Sprintf("f%d", i)); err != nil || len(data) != size {
					t.Errorf("read f%d: %d bytes, %v", i, len(data), err)
					return
				}
			}
		}()
	}

	// A snapshot or clone may answer 409 while writes keep moving the file's blocks.
	retry := func(method, path string, body any) bool {
		for attempt := 0; attempt < 10; attempt++ {
			start := time.Now()
			status, out, err := m.call(method, path, body)
			if err != nil {
				t.Errorf("%s %s after %v: %v (deadlock?)", method, path, time.Since(start), err)
				return false
			}
			switch status {
			case http.StatusOK, http.StatusCreated:
				return true
			case http.StatusConflict:
				time.Sleep(50 * time.Millisecond)
			default:
				t.Errorf("%s %s: %d %s", method, path, status, out)
				return false
			}
		}
		return false
	}
	var snaps, clones int
	for k := 0; k < 4; k++ {
		for i := 0; i < files; i++ {
			src, name := fmt.Sprintf("/f%d", i), fmt.Sprintf("s%d_%d", i, k)
			if retry("POST", "/api/v1/snapshots", map[string]string{"path": src, "name": name}) {
				snaps++
				dest := fmt.Sprintf("/c%d_%d", i, k)
				if retry("POST", "/api/v1/snapshots/"+name+"/clone", map[string]string{"path": src, "dest": dest}) {
					data, err := m.ReadFile(dest[1:])
					if err != nil {
						t.Errorf("read %s: %v", dest, err)
					} else if err := checkChunked(data, i, size); err != nil {
						t.Errorf("clone of snapshot %s: %v", name, err)
					}
					if err := m.Remove(dest[1:]); err != nil {
						t.Error(err)
					}
				}
				retry("DELETE", "/api/v1/snapshots/"+name+"?path="+src, nil)
			}
			dest := fmt.Sprintf("/p%d_%d", i, k)
			if retry("POST", "/api/v1/clones", map[string]string{"path": src, "dest": dest}) {
				clones++
				data, err := m.ReadFile(dest[1:])
				if err != nil {
					t.Errorf("read %s: %v", dest, err)
				} else if err := checkChunked(data, i, size); err != nil {
					t.Errorf("clone of %s: %v", src, err)
				}
				if err := m.Remove(dest[1:]); err != nil {
					t.Error(err)
				}
			}
		}
	}
	close(stop)
	workers.Wait()
	if snaps == 0 || clones == 0 {
		t.Fatalf("no snapshot (%d) or clone (%d) succeeded while files were busy", snaps, clones)
	}

	for i := 0; i < files; i++ {
		data, err := m.ReadFile(fmt.Sprintf("f%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if want := chunked(i, int(lastVersion[i].Load()), size); !bytes.Equal(data, want) {
			t.Errorf("f%d does not hold its last written version %d", i, lastVersion[i].Load())
		}
	}
	if err := m.Unmount(); err != nil {
		t.Fatal(err)
	}
	if code, out := runFsck(t, m.ImagePath); code != 0 {
		t.Fatalf("fsck after API writes during file I/O exited %d:\n%s", code, out)
	}
	t.Logf("%d snapshots and %d clones while files were written", snaps, clones)
}

// TestMountExitsCleanlyDuringSnapshots stops mount with SIGTERM while snapshot requests are running.
func TestMountExitsCleanlyDuringSnapshots(t *testing.T) {
	m := startAPIMount(t, "secret", 80)
	if err := m.WriteFile("big", chunked(0, 1, 32<<20)); err != nil {
		t.Fatal(err)
	}

	var ok atomic.Int64
	var clients sync.WaitGroup
	for g := 0; g < 4; g++ {
		clients.Add(1)
		go func() {
			defer clients.Done()
			for k := 0; ; k++ {
				status, _, err := m.call("POST", "/api/v1/snapshots", map[string]string{"path": "/big", "name": fmt.Sprintf("g%d_%d", g, k)})
				if err != nil {
					return
				}
				if status == http.StatusCreated {
					ok.Add(1)
				}
			}
		}()
	}
	for deadline := time.Now().Add(30 * time.Second); ok.Load() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("no snapshot of /big succeeded within 30 s")
		}
	}
	if err := m.MountCmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	exited := make(chan error, 1)
	go func() { exited <- m.MountCmd.Wait() }()
	select {
	case err := <-exited:
		if err != nil {
			t.Fatalf("mount exited with %v after SIGTERM during snapshots\n%s", err, m.log.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatal("mount did not exit within 60 s of SIGTERM while snapshot requests were running")
	}
	clientsDone := make(chan struct{})
	go func() {
		clients.Wait()
		close(clientsDone)
	}()
	select {
	case <-clientsDone:
	case <-time.After(30 * time.Second):
		t.Fatal("snapshot requests still hanging after mount exited")
	}
	if m.IsMounted() {
		t.Fatal("mount point still mounted after mount exited")
	}
	if code, out := runFsck(t, m.ImagePath); code != 0 {
		t.Fatalf("fsck after shutdown during snapshots exited %d:\n%s", code, out)
	}
	t.Logf("%d snapshots finished before shutdown", ok.Load())
}

func TestAPIAdminOnLiveMount(t *testing.T) {
	m := startAPIMount(t, "secret", 20)
	if err := m.WriteFile("data", bytes.Repeat([]byte("x"), 2<<20)); err != nil {
		t.Fatal(err)
	}
	if err := m.Remove("data"); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"flush-journal", "force-trim", "clear-cache"} {
		if out := m.mustCall(t, http.StatusOK, "POST", "/api/v1/admin/"+op, nil); !strings.Contains(out, `"operation":"`+op+`"`) {
			t.Errorf("admin %s: %s", op, out)
		}
	}
	if err := m.WriteFile("after", []byte("still works")); err != nil {
		t.Fatalf("write after admin calls: %v", err)
	}
	if got, err := m.ReadFile("after"); err != nil || string(got) != "still works" {
		t.Fatalf("read after admin calls: %q %v", got, err)
	}
}

func TestMountTokenSources(t *testing.T) {
	dir := t.TempDir()
	missing := dir + "/missing.img"
	out, err := runMount(t, "-device", missing, "-mount", dir, "-api-token-file", writeTokenFile(t, "secret", 0o644))
	if err == nil || !strings.Contains(out, "chmod 600") || strings.Contains(out, "Failed to open device") {
		t.Errorf("world-readable token file: err=%v\n%s", err, out)
	}
	t.Setenv("GOCLUSTERFS_API_TOKEN", "from-env")
	out, err = runMount(t, "-device", missing, "-mount", dir, "-api-token", "from-flag")
	if err == nil || !strings.Contains(out, "only one") {
		t.Errorf("env and flag together: err=%v\n%s", err, out)
	}

	m := &apiMount{MountedFS: NewMountedFS(t, 20), addr: freeLoopbackAddr(t), token: "from-env", log: &lockedBuffer{}}
	m.Log = m.log
	m.ExtraArgs = []string{"-format", "-api-addr", m.addr}
	if err := m.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		status, _, err := m.call("GET", "/api/v1/snapshots", nil)
		if err == nil && status == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GET /api/v1/snapshots with the env token: %d %v", status, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if strings.Contains(m.log.String(), "WARNING: -api-token is visible") {
		t.Error("warned about -api-token when the token came from the environment")
	}
	_ = os.Unsetenv("GOCLUSTERFS_API_TOKEN")

	out, _ = runMount(t, "-device", missing, "-mount", dir, "-api-token", "from-flag")
	if !strings.Contains(out, "WARNING: -api-token is visible to every local user") {
		t.Errorf("no warning for -api-token:\n%s", out)
	}
}

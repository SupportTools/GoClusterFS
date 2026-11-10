package integration

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find a free port: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

func TestAPIServesHealthOnLoopback(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.Format(); err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	addr := freeLoopbackAddr(t)
	mfs.ExtraArgs = []string{"-api-addr", addr}
	if err := mfs.Mount(false); err != nil {
		t.Fatalf("Mount failed: %v", err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := client.Get("http://" + addr + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET /health returned %d, want 200", resp.StatusCode)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("API never answered on %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestMountRefusesUnauthenticatedRemoteAPI(t *testing.T) {
	binary, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}
	mountPoint := t.TempDir()
	image := mountPoint + ".img"
	if err := allocateImage(image, 20); err != nil {
		t.Fatalf("Failed to allocate image: %v", err)
	}
	t.Cleanup(func() { os.Remove(image) })

	for _, addr := range []string{":8080", "0.0.0.0:8080"} {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		out, err := exec.CommandContext(ctx, binary, "-device", image, "-mount", mountPoint, "-format", "-api-addr", addr).CombinedOutput()
		timedOut := ctx.Err() != nil
		cancel()
		if timedOut {
			exec.Command("fusermount", "-u", mountPoint).Run()
			t.Fatalf("-api-addr %s without -api-token: mount kept running", addr)
		}
		if err == nil {
			t.Fatalf("-api-addr %s without -api-token: mount exited 0", addr)
		}
		if !strings.Contains(string(out), "-api-token") {
			t.Errorf("-api-addr %s: error does not mention -api-token:\n%s", addr, out)
		}
	}

	f, err := os.Open(image)
	if err != nil {
		t.Fatalf("Failed to open image: %v", err)
	}
	defer f.Close()
	head := make([]byte, 4096)
	if _, err := io.ReadFull(f, head); err != nil {
		t.Fatalf("Failed to read image: %v", err)
	}
	if !bytes.Equal(head, make([]byte, len(head))) {
		t.Error("refused mount still wrote a superblock")
	}
}

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeLimit(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The limit is the smallest set on the way from the process's cgroup to the root: a pod's limit above a container
// left at "max" counts, and with none set there is no limit. cgroup v1 reads memory.limit_in_bytes, where unlimited
// is a value near 2^63.
func TestCgroupMemoryLimit(t *testing.T) {
	v2 := t.TempDir()
	writeLimit(t, filepath.Join(v2, "kubepods/pod1/memory.max"), "2147483648")
	writeLimit(t, filepath.Join(v2, "kubepods/pod1/ctr/memory.max"), "max")
	writeLimit(t, filepath.Join(v2, "kubepods/memory.max"), "8589934592")
	writeLimit(t, filepath.Join(v2, "free/memory.max"), "max")
	v1 := t.TempDir()
	writeLimit(t, filepath.Join(v1, "memory/docker/abc/memory.limit_in_bytes"), "1073741824")
	writeLimit(t, filepath.Join(v1, "memory/docker/memory.limit_in_bytes"), "9223372036854771712")

	for _, c := range []struct {
		name, root, self string
		want             uint64
	}{
		{"v2, the pod's limit", v2, "0::/kubepods/pod1/ctr\n", 2 << 30},
		{"v2, no limit", v2, "0::/free\n", 0},
		{"v2, a missing cgroup", v2, "0::/gone\n", 0},
		{"v1", v1, "12:cpu,cpuacct:/docker/abc\n7:memory:/docker/abc\n", 1 << 30},
		{"v1 without a memory controller", v1, "12:cpu:/docker/abc\n", 0},
	} {
		if got := cgroupMemoryLimit(c.root, c.self); got != c.want {
			t.Errorf("%s: limit %d, want %d", c.name, got, c.want)
		}
	}
}

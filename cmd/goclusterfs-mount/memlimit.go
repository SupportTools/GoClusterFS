package main

import (
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
)

// memoryLimitShare is the part of the cgroup's memory limit the Go heap aims to stay under, leaving the rest for
// stacks, the runtime and the kernel's charges to the cgroup.
const memoryLimitShare = 0.9

// setMemoryLimit sets the Go memory limit from the cgroup's memory limit, unless GOMEMLIMIT is set, so the garbage
// collector works harder before the limit kills the mount.
func setMemoryLimit() {
	if v := os.Getenv("GOMEMLIMIT"); v != "" {
		log.Printf("Memory: GOMEMLIMIT=%s", v)
		return
	}
	self, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return
	}
	limit := cgroupMemoryLimit("/sys/fs/cgroup", string(self))
	if limit == 0 {
		return
	}
	goal := int64(float64(limit) * memoryLimitShare)
	debug.SetMemoryLimit(goal)
	log.Printf("Memory: the cgroup allows %d MB, so the Go memory limit is %d MB; set GOMEMLIMIT to choose another", limit>>20, goal>>20)
}

// cgroupMemoryLimit returns the smallest memory limit on the way from this process's cgroup up to the root, as
// /proc/self/cgroup lists it under root, or 0 when none is set. It reads memory.max on cgroup v2 and
// memory.limit_in_bytes on v1.
func cgroupMemoryLimit(root, self string) uint64 {
	var limit uint64
	for line := range strings.Lines(self) {
		fields := strings.SplitN(strings.TrimSpace(line), ":", 3)
		if len(fields) != 3 {
			continue
		}
		dir, file := root, "memory.max"
		switch {
		case fields[0] == "0" && fields[1] == "":
		case slices.Contains(strings.Split(fields[1], ","), "memory"):
			dir, file = filepath.Join(root, "memory"), "memory.limit_in_bytes"
		default:
			continue
		}
		for p := filepath.Join(dir, fields[2]); ; p = filepath.Dir(p) {
			if n := readLimit(filepath.Join(p, file)); n != 0 && (limit == 0 || n < limit) {
				limit = n
			}
			if p == dir || len(p) <= len(dir) {
				break
			}
		}
	}
	return limit
}

// readLimit reads a cgroup memory limit file; "max", a v1 value near 2^63, or an unreadable file is no limit.
func readLimit(path string) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil || n >= 1<<62 {
		return 0
	}
	return n
}

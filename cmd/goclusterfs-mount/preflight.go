package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/supporttools/GoClusterFS/internal/cache/broadcast"
	"github.com/supporttools/GoClusterFS/internal/cli"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

const (
	statusPass = "PASS"
	statusWarn = "WARN"
	statusFail = "FAIL"
)

type check struct {
	name, status, detail, fix string
}

type preflightConfig struct {
	device, mountPoint string
	format             bool
	network            broadcast.Config
	apiAddr            string // empty when the API is off
	fuseDevice         string
	lookPath           func(string) (string, error)
	uid                int
}

// runPreflight checks what a mount needs from this host and the device,
// changing nothing, and returns the exit code.
func runPreflight(w io.Writer, cfg preflightConfig) int {
	checks := []check{
		checkFuseDevice(cfg.fuseDevice),
		checkFusermount(cfg.lookPath, cfg.uid),
		checkMountPoint(cfg.mountPoint),
		checkDevice(cfg.device, cfg.format),
	}
	checks = append(checks, checkBroadcast(cfg.network)...)
	checks = append(checks, checkAPIAddr(cfg.apiAddr))

	failed, warned := 0, 0
	for _, c := range checks {
		_, _ = fmt.Fprintf(w, "%s  %-17s %s\n", c.status, c.name, c.detail)
		if c.fix != "" {
			_, _ = fmt.Fprintf(w, "      %-17s fix: %s\n", "", c.fix)
		}
		switch c.status {
		case statusFail:
			failed++
		case statusWarn:
			warned++
		}
	}
	switch {
	case failed > 0:
		_, _ = fmt.Fprintf(w, "\n%d of %d checks failed\n", failed, len(checks))
		return cli.ExitFailure
	case warned > 0:
		_, _ = fmt.Fprintf(w, "\nno check failed; read the %d warnings\n", warned)
	default:
		_, _ = fmt.Fprintf(w, "\nall %d checks passed\n", len(checks))
	}
	return cli.ExitOK
}

func checkFuseDevice(path string) check {
	c := check{name: "FUSE device"}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	switch {
	case errors.Is(err, os.ErrNotExist):
		c.status, c.detail = statusFail, path+" does not exist"
		c.fix = "load the FUSE kernel module: modprobe fuse"
	case errors.Is(err, os.ErrPermission):
		c.status, c.detail = statusFail, path+" is not readable and writable by this user"
		c.fix = "run as root, or give this user read and write access to " + path
	case err != nil:
		c.status, c.detail = statusFail, fmt.Sprintf("opening %s failed: %v", path, err)
		c.fix = "check that the FUSE kernel module is loaded: modprobe fuse"
	default:
		_ = f.Close()
		c.status, c.detail = statusPass, path+" opens read-write"
	}
	return c
}

// checkFusermount finds the helper go-fuse mounts with: fusermount3, else
// fusermount. A user other than root needs it setuid root.
func checkFusermount(lookPath func(string) (string, error), uid int) check {
	c := check{name: "fusermount"}
	path, err := lookPath("fusermount3")
	if err != nil {
		path, err = lookPath("fusermount")
	}
	if err != nil {
		c.status, c.detail = statusFail, "neither fusermount3 nor fusermount is in PATH"
		c.fix = "install fuse3 (apt install fuse3, or dnf install fuse3)"
		return c
	}
	if uid == 0 {
		c.status, c.detail = statusPass, path+" (running as root)"
		return c
	}
	info, err := os.Stat(path)
	if err != nil {
		c.status, c.detail = statusFail, fmt.Sprintf("stat %s: %v", path, err)
		c.fix = "reinstall fuse3, or run as root"
		return c
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSetuid == 0 || !ok || st.Uid != 0 {
		c.status, c.detail = statusFail, path+" is not setuid root, so this user cannot mount"
		c.fix = "run as root, or reinstall fuse3 so " + path + " is setuid root"
		return c
	}
	c.status, c.detail = statusPass, path+" is setuid root"
	return c
}

func checkMountPoint(dir string) check {
	c := check{name: "mount point"}
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		c.status, c.detail, c.fix = statusFail, dir+" does not exist", "mkdir -p "+dir
		return c
	case errors.Is(err, syscall.ENOTCONN):
		c.status, c.detail = statusFail, dir+" holds a FUSE mount whose process is gone"
		c.fix = "fusermount3 -u " + dir
		return c
	case err != nil:
		c.status, c.detail, c.fix = statusFail, fmt.Sprintf("stat %s: %v", dir, err), "check the path and its permissions"
		return c
	case !info.IsDir():
		c.status, c.detail, c.fix = statusFail, dir+" is not a directory", "pass a directory with -mount"
		return c
	}
	if parent, err := os.Stat(filepath.Dir(filepath.Clean(dir))); err == nil && !sameDevice(info, parent) {
		c.status, c.detail = statusFail, "something is already mounted on "+dir
		c.fix = "unmount it (fusermount3 -u " + dir + " or umount " + dir + "), or pass another directory with -mount"
		return c
	}
	c.status, c.detail = statusPass, dir+" is a directory with nothing mounted on it"
	return c
}

func sameDevice(a, b os.FileInfo) bool {
	sa, okA := a.Sys().(*syscall.Stat_t)
	sb, okB := b.Sys().(*syscall.Stat_t)
	return !okA || !okB || sa.Dev == sb.Dev
}

// checkDevice opens the device as a mount does, with O_DIRECT, and reads the superblock.
func checkDevice(path string, format bool) check {
	c := check{name: "device"}
	dev, err := storage.Open(path)
	if err != nil {
		c.status, c.detail, c.fix = statusFail, fmt.Sprintf("cannot open %s with O_DIRECT: %v", path, err), cli.OpenHint(err)
		return c
	}
	defer func() { _ = dev.Close() }()
	size := uint64(dev.Size())
	if size < superblock.MinDeviceSize {
		c.status, c.detail = statusFail, fmt.Sprintf("%s holds %d MB, less than the %d MB a filesystem needs", path, size>>20, superblock.MinDeviceSize>>20)
		c.fix = "use a larger device or image"
		return c
	}
	if format {
		c.status, c.detail = statusWarn, fmt.Sprintf("%s (%d MB) opens with O_DIRECT; -format erases all data on it", path, size>>20)
		return c
	}
	sb, err := superblock.Read(dev)
	switch {
	case errors.Is(err, superblock.ErrChecksumMismatch):
		c.status, c.detail = statusFail, fmt.Sprintf("the superblock of %s is damaged: %v", path, err)
		c.fix = "check it with goclusterfs-fsck " + path
	case err != nil:
		c.status, c.detail = statusFail, fmt.Sprintf("%s has no filesystem this version can mount: %v", path, err)
		c.fix = "check -device; to create a new filesystem, add -format, which erases the device"
	default:
		c.status, c.detail = statusPass, fmt.Sprintf("%s (%d MB) opens with O_DIRECT and holds a version %d filesystem", path, size>>20, sb.Version)
	}
	return c
}

func checkBroadcast(cfg broadcast.Config) []check {
	if !cfg.Enabled {
		return []check{{name: "broadcast ports", status: statusPass, detail: "broadcast is off (-broadcast=false)"}}
	}
	ports := check{name: "broadcast ports"}
	if err := broadcast.CheckPorts(cfg); err != nil {
		ports.status, ports.detail = statusFail, err.Error()
		ports.fix = fmt.Sprintf("free UDP ports %d and %d, choose others with -mcast-port and -unicast-port, or pass -broadcast=false", cfg.MulticastPort, cfg.UnicastPort)
	} else {
		ports.status, ports.detail = statusPass, fmt.Sprintf("UDP ports %d (multicast) and %d (unicast) are free", cfg.MulticastPort, cfg.UnicastPort)
	}
	if cfg.Secret != "" {
		return []check{ports}
	}
	return []check{ports, {
		name:   "broadcast secret",
		status: statusWarn,
		detail: "broadcast messages are not authenticated",
		fix:    "set the same -broadcast-secret on every node",
	}}
}

func checkAPIAddr(addr string) check {
	c := check{name: "API address"}
	if addr == "" {
		c.status, c.detail = statusPass, "the API is off (-api=false)"
		return c
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		c.status, c.detail = statusFail, fmt.Sprintf("cannot listen on %s: %v", addr, err)
		c.fix = "choose a free address with -api-addr, or pass -api=false"
		return c
	}
	_ = l.Close()
	c.status, c.detail = statusPass, addr+" is free"
	return c
}

func defaultPreflightConfig() preflightConfig {
	return preflightConfig{fuseDevice: "/dev/fuse", lookPath: lookPathWithBin, uid: os.Getuid()}
}

// lookPathWithBin searches PATH, then /bin, as go-fuse does for fusermount.
func lookPathWithBin(file string) (string, error) {
	if path, err := exec.LookPath(file); err == nil {
		return path, nil
	}
	return exec.LookPath(filepath.Join("/bin", file))
}

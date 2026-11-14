// Package integration provides integration tests for GoClusterFS.
// These tests mount actual filesystems and perform real file operations.
package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// MountedFS manages a temporary mounted GoClusterFS instance for testing.
type MountedFS struct {
	ImagePath  string
	MountPoint string
	ExtraArgs  []string  // Appended to the mount command by Mount and FormatAndMount
	Log        io.Writer // Also receives the mount process output when set
	Env        []string  // Added to the mount process environment
	MountCmd   *exec.Cmd
	BinaryPath string
	t          *testing.T
	ctx        context.Context
	cancel     context.CancelFunc

	// portArgs give the node its own broadcast and API ports; ExtraArgs come after and win.
	portArgs []string
}

// freeUDPPorts returns n distinct UDP ports that were free on all interfaces.
func freeUDPPorts(t *testing.T, n int) []int {
	t.Helper()
	ports := make([]int, n)
	for i := range ports {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{})
		if err != nil {
			t.Fatalf("Failed to find a free UDP port: %v", err)
		}
		defer func() { _ = conn.Close() }()
		ports[i] = conn.LocalAddr().(*net.UDPAddr).Port
	}
	return ports
}

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find a free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// portArgs sets the node's ports. Nodes of one cluster share the multicast
// port, every node needs its own unicast port, and a mount fails when it
// cannot bind them. The API gets a free port instead of the default 8080.
func portArgs(t *testing.T, mcastPort, unicastPort int) []string {
	return []string{
		"-mcast-port", strconv.Itoa(mcastPort),
		"-unicast-port", strconv.Itoa(unicastPort),
		"-api-addr", freeLoopbackAddr(t),
	}
}

// findBinary locates the goclusterfs-mount binary.
// It searches in the project root first, then PATH.
func findBinary() (string, error) {
	// Try project root first
	projectRoot := filepath.Join(os.Getenv("PWD"), "..", "..")
	binaryPath := filepath.Join(projectRoot, "goclusterfs-mount")
	if _, err := os.Stat(binaryPath); err == nil {
		return binaryPath, nil
	}

	// Try relative to current directory
	if _, err := os.Stat("../../goclusterfs-mount"); err == nil {
		return "../../goclusterfs-mount", nil
	}

	// Try absolute path (common during development)
	absPath := "/home/mmattox/go/src/github.com/supporttools/GoClusterFS/goclusterfs-mount"
	if _, err := os.Stat(absPath); err == nil {
		return absPath, nil
	}

	// Try PATH
	path, err := exec.LookPath("goclusterfs-mount")
	if err == nil {
		return path, nil
	}

	return "", fmt.Errorf("goclusterfs-mount binary not found; run 'go build -o goclusterfs-mount ./cmd/goclusterfs-mount' first")
}

// NewMountedFS creates and mounts a new test filesystem.
// The filesystem is automatically cleaned up when the test completes.
func NewMountedFS(t *testing.T, sizeMB int) *MountedFS {
	t.Helper()

	binaryPath, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}

	// Create temporary image file
	imgFile, err := os.CreateTemp("", "goclusterfs_test_*.img")
	if err != nil {
		t.Fatalf("Failed to create temp image: %v", err)
	}
	imagePath := imgFile.Name()
	mustClose(t, imgFile)

	// Allocate the image file
	if err := allocateImage(imagePath, sizeMB); err != nil {
		_ = os.Remove(imagePath)
		t.Fatalf("Failed to allocate image: %v", err)
	}
	t.Logf("Created %dMB image at %s", sizeMB, imagePath)

	// Create mount point directory
	mountPoint, err := os.MkdirTemp("", "goclusterfs_mnt_")
	if err != nil {
		_ = os.Remove(imagePath)
		t.Fatalf("Failed to create mount point: %v", err)
	}
	t.Logf("Mount point: %s", mountPoint)

	ctx, cancel := context.WithCancel(context.Background())

	ports := freeUDPPorts(t, 2)
	mfs := &MountedFS{
		ImagePath:  imagePath,
		MountPoint: mountPoint,
		BinaryPath: binaryPath,
		t:          t,
		ctx:        ctx,
		cancel:     cancel,
		portArgs:   portArgs(t, ports[0], ports[1]),
	}

	// Register cleanup
	t.Cleanup(func() {
		mfs.Cleanup()
	})

	return mfs
}

// allocateImage creates a sparse file with sizeMB of data space after the metadata regions.
func allocateImage(path string, sizeMB int) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	return errors.Join(f.Truncate(superblock.MinDeviceSize+int64(sizeMB)*1024*1024), f.Close())
}

// Format formats the filesystem by running mount with -format and then killing it
// after formatting completes. The mount command formats AND mounts, so we need to
// wait for the format to complete, then unmount.
func (m *MountedFS) Format() error {
	args := []string{
		"-device", m.ImagePath,
		"-mount", m.MountPoint,
		"-format",
	}
	args = append(args, m.portArgs...)

	cmd := exec.Command(m.BinaryPath, args...)

	// Capture output to detect when formatting is done
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %v", err)
	}
	cmd.Stderr = cmd.Stdout // Combine stderr into stdout

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start format command: %v", err)
	}

	// Read output and wait for "Filesystem formatted successfully" or "Filesystem mounted"
	buf := make([]byte, 4096)
	var output strings.Builder
	formatted := false

	for {
		n, err := stdout.Read(buf)
		if n > 0 {
			output.Write(buf[:n])
			m.t.Logf("[format] %s", string(buf[:n]))
			if strings.Contains(output.String(), "Filesystem formatted successfully") {
				formatted = true
			}
			if strings.Contains(output.String(), "Filesystem mounted") {
				// Mount started, we can unmount now
				break
			}
		}
		if err != nil {
			break
		}
	}

	// Unmount the filesystem that was started during format
	unmountCmd := exec.Command("fusermount", "-u", m.MountPoint)
	_ = unmountCmd.Run()

	// Kill the process
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	if !formatted {
		return fmt.Errorf("format failed, output: %s", output.String())
	}

	m.t.Log("Filesystem formatted successfully")
	return nil
}

func (m *MountedFS) output() io.Writer {
	if m.Log == nil {
		return os.Stdout
	}
	return io.MultiWriter(os.Stdout, m.Log)
}

// Mount mounts the filesystem in the background.
func (m *MountedFS) Mount(debug bool) error {
	args := []string{
		"-device", m.ImagePath,
		"-mount", m.MountPoint,
	}
	if debug {
		args = append(args, "-debug")
	}
	args = append(args, m.portArgs...)
	args = append(args, m.ExtraArgs...)

	// Create a fresh context for this mount operation
	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout
	m.MountCmd.Env = m.environ()

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		_ = m.Unmount()
		return err
	}

	m.t.Logf("Mounted at %s", m.MountPoint)
	return nil
}

// FormatAndMount formats and mounts in a single operation.
// This is more efficient as the mount command does both with -format.
func (m *MountedFS) FormatAndMount(debug bool) error {
	args := []string{
		"-device", m.ImagePath,
		"-mount", m.MountPoint,
		"-format",
	}
	if debug {
		args = append(args, "-debug")
	}
	args = append(args, m.portArgs...)
	args = append(args, m.ExtraArgs...)

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout
	m.MountCmd.Env = m.environ()

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		_ = m.Unmount()
		return err
	}

	m.t.Logf("Formatted and mounted at %s", m.MountPoint)
	return nil
}

// WaitForMount waits until the filesystem is mounted and accessible.
// It checks /proc/mounts to verify the FUSE mount is active.
func (m *MountedFS) WaitForMount(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Check if mounted via /proc/mounts
		if m.IsMounted() {
			// Also verify we can access it
			entries, err := os.ReadDir(m.MountPoint)
			if err == nil {
				m.t.Logf("Mount accessible, entries: %d", len(entries))
				return nil
			}
		}

		// Check if mount command exited unexpectedly
		if m.MountCmd != nil && m.MountCmd.ProcessState != nil {
			return fmt.Errorf("mount process exited unexpectedly: %v", m.MountCmd.ProcessState)
		}

		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for mount after %v", timeout)
}

// unmountBusyTimeout is how long Unmount retries fusermount while the mount is
// busy: a FUSE release still in flight after close, or a desktop indexer
// crawling the mount, keeps it busy for a moment.
const unmountBusyTimeout = 10 * time.Second

// Unmount unmounts the filesystem and waits for the mount process to run its
// clean shutdown. If the mount stays busy, it kills the process and detaches
// the dead mount, so a later Mount at the same path can work, and returns an
// error.
func (m *MountedFS) Unmount() error {
	var unmountErr error
	start := time.Now()
	for busy := 0; ; busy++ {
		output, err := exec.Command("fusermount", "-u", m.MountPoint).CombinedOutput()
		if err == nil || !m.IsMounted() {
			unmountErr = nil
			if busy > 0 {
				m.t.Logf("fusermount -u succeeded after the mount was busy for %v", time.Since(start).Round(time.Millisecond))
			}
			break
		}
		unmountErr = fmt.Errorf("fusermount -u %s: %v: %s", m.MountPoint, err, strings.TrimSpace(string(output)))
		if !strings.Contains(string(output), "busy") || time.Since(start) > unmountBusyTimeout {
			m.t.Log(unmountErr)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Do not cancel the context first: that kills the process before its
	// clean shutdown checkpoints the journal.
	if m.MountCmd != nil && m.MountCmd.Process != nil {
		done := make(chan error, 1)
		go func() {
			done <- m.MountCmd.Wait()
		}()

		select {
		case err := <-done:
			if err != nil {
				m.t.Logf("mount process exited: %v", err)
			}
		case <-time.After(10 * time.Second):
			m.t.Log("Warning: mount process did not exit in time, force killing")
			if m.cancel != nil {
				m.cancel()
			}
			_ = m.MountCmd.Process.Kill()
		}
	}

	if m.IsMounted() {
		if output, err := exec.Command("fusermount", "-uz", m.MountPoint).CombinedOutput(); err != nil {
			m.t.Logf("fusermount -uz %s: %v: %s", m.MountPoint, err, output)
		}
	}
	if unmountErr != nil {
		return unmountErr
	}
	m.t.Log("Unmounted successfully")
	return nil
}

// Cleanup unmounts and removes all temporary files.
func (m *MountedFS) Cleanup() {
	// Unmount first
	_ = m.Unmount()

	// Remove mount point directory
	if m.MountPoint != "" {
		_ = os.RemoveAll(m.MountPoint)
	}

	// Remove image file
	if m.ImagePath != "" {
		_ = os.Remove(m.ImagePath)
	}
}

// IsMounted checks if the filesystem is currently mounted.
func (m *MountedFS) IsMounted() bool {
	// Check /proc/mounts for our mount point
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false
	}
	return strings.Contains(string(data), m.MountPoint)
}

// environ is the mount process environment: the test's, plus Env.
func (m *MountedFS) environ() []string {
	if len(m.Env) == 0 {
		return nil
	}
	return append(os.Environ(), m.Env...)
}

// Path returns the full path for a file relative to the mount point.
func (m *MountedFS) Path(name string) string {
	return filepath.Join(m.MountPoint, name)
}

// WriteFile writes data to a file in the mounted filesystem.
func (m *MountedFS) WriteFile(name string, data []byte) error {
	return os.WriteFile(m.Path(name), data, 0644)
}

// WriteFileAt writes data to a file at a specific offset.
// This allows testing concurrent writes to the same file at different positions.
func (m *MountedFS) WriteFileAt(name string, data []byte, offset int64) error {
	f, err := os.OpenFile(m.Path(name), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, offset)
	return errors.Join(err, f.Close())
}

// ReadFile reads data from a file in the mounted filesystem.
func (m *MountedFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(m.Path(name))
}

// Mkdir creates a directory in the mounted filesystem.
func (m *MountedFS) Mkdir(name string) error {
	return os.Mkdir(m.Path(name), 0755)
}

// MkdirAll creates a directory and all parents in the mounted filesystem.
func (m *MountedFS) MkdirAll(name string) error {
	return os.MkdirAll(m.Path(name), 0755)
}

// Remove removes a file or empty directory.
func (m *MountedFS) Remove(name string) error {
	return os.Remove(m.Path(name))
}

// RemoveAll removes a file or directory and all children.
func (m *MountedFS) RemoveAll(name string) error {
	return os.RemoveAll(m.Path(name))
}

// ReadDir reads a directory and returns its entries.
func (m *MountedFS) ReadDir(name string) ([]os.DirEntry, error) {
	if name == "" {
		return os.ReadDir(m.MountPoint)
	}
	return os.ReadDir(m.Path(name))
}

// Stat returns file info for a path.
func (m *MountedFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(m.Path(name))
}

// Exists checks if a file or directory exists.
func (m *MountedFS) Exists(name string) bool {
	_, err := os.Stat(m.Path(name))
	return err == nil
}

// NewMountedFSWithImage creates a MountedFS that uses a pre-existing image file.
// This is useful for multi-node tests where multiple mounts share the same image.
// The image is NOT cleaned up by this instance - the caller manages the shared image.
func NewMountedFSWithImage(t *testing.T, imagePath string) *MountedFS {
	t.Helper()

	binaryPath, err := findBinary()
	if err != nil {
		t.Fatalf("Failed to find binary: %v", err)
	}

	// Create mount point directory
	mountPoint, err := os.MkdirTemp("", "goclusterfs_mnt_")
	if err != nil {
		t.Fatalf("Failed to create mount point: %v", err)
	}
	t.Logf("Mount point: %s (shared image: %s)", mountPoint, imagePath)

	ctx, cancel := context.WithCancel(context.Background())

	ports := freeUDPPorts(t, 2)
	mfs := &MountedFS{
		ImagePath:  imagePath,
		MountPoint: mountPoint,
		BinaryPath: binaryPath,
		t:          t,
		ctx:        ctx,
		cancel:     cancel,
		portArgs:   portArgs(t, ports[0], ports[1]),
	}

	// Register cleanup - only removes mount point, NOT the shared image
	t.Cleanup(func() {
		mfs.CleanupMountOnly()
	})

	return mfs
}

// CleanupMountOnly unmounts and removes the mount point but preserves the image.
// Used for shared image scenarios where multiple mounts use the same backing file.
func (m *MountedFS) CleanupMountOnly() {
	// Unmount first
	_ = m.Unmount()

	// Remove mount point directory only
	if m.MountPoint != "" {
		_ = os.RemoveAll(m.MountPoint)
	}
	// Note: ImagePath is NOT removed - caller manages shared image
}

// PID returns the process ID of the mount command, or 0 if not running.
func (m *MountedFS) PID() int {
	if m.MountCmd != nil && m.MountCmd.Process != nil {
		return m.MountCmd.Process.Pid
	}
	return 0
}

// Kill sends SIGKILL to the mount process to simulate a crash and waits until it is gone.
func (m *MountedFS) Kill() error {
	if m.MountCmd == nil || m.MountCmd.Process == nil {
		return fmt.Errorf("mount process not running")
	}
	if err := m.MountCmd.Process.Kill(); err != nil {
		return err
	}
	exited := make(chan struct{})
	go func() {
		_ = m.MountCmd.Wait()
		close(exited)
	}()
	select {
	case <-exited:
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("mount process %d still running 10s after SIGKILL", m.MountCmd.Process.Pid)
	}
}

// OpenFile opens a file in the mounted filesystem for read/write operations.
func (m *MountedFS) OpenFile(name string) (*os.File, error) {
	return os.OpenFile(m.Path(name), os.O_RDWR, 0644)
}

// FormatAndMountWithSFB formats and mounts with SFB (Sub-File Block) containers enabled.
// SFB provides efficient storage for small files by packing them into shared 1MB blocks.
func (m *MountedFS) FormatAndMountWithSFB(debug bool) error {
	args := []string{
		"-device", m.ImagePath,
		"-mount", m.MountPoint,
		"-format",
		"-sfb=true",
	}
	if debug {
		args = append(args, "-debug")
	}
	args = append(args, m.portArgs...)

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout
	m.MountCmd.Env = m.environ()

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		_ = m.Unmount()
		return err
	}

	m.t.Logf("Formatted and mounted at %s with SFB enabled", m.MountPoint)
	return nil
}

// MountWithSFB mounts the filesystem with SFB enabled (no format).
func (m *MountedFS) MountWithSFB(debug bool) error {
	args := []string{
		"-device", m.ImagePath,
		"-mount", m.MountPoint,
		"-sfb=true",
	}
	if debug {
		args = append(args, "-debug")
	}
	args = append(args, m.portArgs...)

	// Create a fresh context for this mount operation
	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout
	m.MountCmd.Env = m.environ()

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		_ = m.Unmount()
		return err
	}

	m.t.Logf("Mounted at %s with SFB enabled", m.MountPoint)
	return nil
}

// mustClose reports a failed close. On a FUSE mount, closing a written file
// returns the errors from flushing it.
func mustClose(tb testing.TB, c io.Closer) {
	tb.Helper()
	if err := c.Close(); err != nil {
		tb.Errorf("close: %v", err)
	}
}

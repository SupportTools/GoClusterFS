// Package integration provides integration tests for GoClusterFS.
// These tests mount actual filesystems and perform real file operations.
package integration

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// MountedFS manages a temporary mounted GoClusterFS instance for testing.
type MountedFS struct {
	ImagePath  string
	MountPoint string
	ExtraArgs  []string  // Appended to the mount command by Mount
	Log        io.Writer // Also receives the mount process output when set
	MountCmd   *exec.Cmd
	BinaryPath string
	t          *testing.T
	ctx        context.Context
	cancel     context.CancelFunc
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

	return "", fmt.Errorf("goclusterfs-mount binary not found; run 'go build -o goclusterfs-mount ./cmd/mount' first")
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
	imgFile.Close()

	// Allocate the image file
	if err := allocateImage(imagePath, sizeMB); err != nil {
		os.Remove(imagePath)
		t.Fatalf("Failed to allocate image: %v", err)
	}
	t.Logf("Created %dMB image at %s", sizeMB, imagePath)

	// Create mount point directory
	mountPoint, err := os.MkdirTemp("", "goclusterfs_mnt_")
	if err != nil {
		os.Remove(imagePath)
		t.Fatalf("Failed to create mount point: %v", err)
	}
	t.Logf("Mount point: %s", mountPoint)

	ctx, cancel := context.WithCancel(context.Background())

	mfs := &MountedFS{
		ImagePath:  imagePath,
		MountPoint: mountPoint,
		BinaryPath: binaryPath,
		t:          t,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Register cleanup
	t.Cleanup(func() {
		mfs.Cleanup()
	})

	return mfs
}

// allocateImage creates a sparse file of the specified size.
func allocateImage(path string, sizeMB int) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	return f.Truncate(int64(sizeMB) * 1024 * 1024)
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
	unmountCmd.Run() // Ignore error - might not be mounted yet

	// Kill the process
	if cmd.Process != nil {
		cmd.Process.Kill()
		cmd.Wait()
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
	args = append(args, m.ExtraArgs...)

	// Create a fresh context for this mount operation
	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		m.Unmount() // Try to clean up
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

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		m.Unmount() // Try to clean up
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

// Unmount unmounts the filesystem using fusermount.
func (m *MountedFS) Unmount() error {
	// Use fusermount to unmount FIRST.
	// This triggers the go-fuse server to exit cleanly, allowing
	// the mount command to run its shutdown code (journal checkpoint, etc.)
	// Do NOT cancel the context first - that would SIGKILL the process
	// and skip the clean shutdown path where journal checkpointing happens.
	cmd := exec.Command("fusermount", "-u", m.MountPoint)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// May fail if already unmounted
		m.t.Logf("fusermount output: %s", output)
	}

	// Wait for mount process to exit cleanly
	if m.MountCmd != nil && m.MountCmd.Process != nil {
		// Give it time to run clean shutdown (journal checkpoint, etc.)
		done := make(chan error, 1)
		go func() {
			done <- m.MountCmd.Wait()
		}()

		select {
		case <-done:
			// Process exited cleanly
		case <-time.After(10 * time.Second):
			// Force kill only as last resort after generous timeout
			m.t.Log("Warning: mount process did not exit in time, force killing")
			if m.cancel != nil {
				m.cancel()
			}
			m.MountCmd.Process.Kill()
		}
	}

	m.t.Log("Unmounted successfully")
	return nil
}

// Cleanup unmounts and removes all temporary files.
func (m *MountedFS) Cleanup() {
	// Unmount first
	m.Unmount()

	// Remove mount point directory
	if m.MountPoint != "" {
		os.RemoveAll(m.MountPoint)
	}

	// Remove image file
	if m.ImagePath != "" {
		os.Remove(m.ImagePath)
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
	defer f.Close()

	_, err = f.WriteAt(data, offset)
	return err
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

	mfs := &MountedFS{
		ImagePath:  imagePath,
		MountPoint: mountPoint,
		BinaryPath: binaryPath,
		t:          t,
		ctx:        ctx,
		cancel:     cancel,
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
	m.Unmount()

	// Remove mount point directory only
	if m.MountPoint != "" {
		os.RemoveAll(m.MountPoint)
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

// Kill sends SIGKILL to the mount process to simulate a crash.
func (m *MountedFS) Kill() error {
	if m.MountCmd == nil || m.MountCmd.Process == nil {
		return fmt.Errorf("mount process not running")
	}
	return m.MountCmd.Process.Kill()
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

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		m.Unmount() // Try to clean up
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

	// Create a fresh context for this mount operation
	m.ctx, m.cancel = context.WithCancel(context.Background())

	m.MountCmd = exec.CommandContext(m.ctx, m.BinaryPath, args...)
	m.MountCmd.Stdout = m.output()
	m.MountCmd.Stderr = m.MountCmd.Stdout

	if err := m.MountCmd.Start(); err != nil {
		return fmt.Errorf("failed to start mount command: %v", err)
	}

	// Wait for mount to become accessible
	if err := m.WaitForMount(10 * time.Second); err != nil {
		m.Unmount() // Try to clean up
		return err
	}

	m.t.Logf("Mounted at %s with SFB enabled", m.MountPoint)
	return nil
}

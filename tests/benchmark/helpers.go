// Package benchmark provides performance benchmarks for GoClusterFS.
package benchmark

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// BenchmarkFS manages a temporary mounted GoClusterFS instance for benchmarking.
type BenchmarkFS struct {
	ImagePath  string
	MountPoint string
	MountCmd   *exec.Cmd
	BinaryPath string
	b          *testing.B
	ctx        context.Context
	cancel     context.CancelFunc
}

// findBinary locates the goclusterfs-mount binary.
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

	// Try absolute path
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

// allocateImage creates a sparse image file with sizeMB of data space after the metadata regions.
func allocateImage(path string, sizeMB int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Truncate(superblock.MinDeviceSize + int64(sizeMB)*1024*1024)
}

// setupBenchmarkFS creates and mounts a new test filesystem for benchmarking.
func setupBenchmarkFS(b *testing.B, sizeMB int) *BenchmarkFS {
	b.Helper()

	binaryPath, err := findBinary()
	if err != nil {
		b.Fatalf("Failed to find binary: %v", err)
	}

	// Create temporary image file
	imgFile, err := os.CreateTemp("", "goclusterfs_bench_*.img")
	if err != nil {
		b.Fatalf("Failed to create temp image: %v", err)
	}
	imagePath := imgFile.Name()
	imgFile.Close()

	// Allocate the image file
	if err := allocateImage(imagePath, sizeMB); err != nil {
		os.Remove(imagePath)
		b.Fatalf("Failed to allocate image: %v", err)
	}

	// Create mount point directory
	mountPoint, err := os.MkdirTemp("", "goclusterfs_bench_mnt_")
	if err != nil {
		os.Remove(imagePath)
		b.Fatalf("Failed to create mount point: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	mfs := &BenchmarkFS{
		ImagePath:  imagePath,
		MountPoint: mountPoint,
		BinaryPath: binaryPath,
		b:          b,
		ctx:        ctx,
		cancel:     cancel,
	}

	// Format and mount
	mfs.formatAndMount()

	return mfs
}

// formatAndMount formats and mounts the filesystem.
func (mfs *BenchmarkFS) formatAndMount() {
	mfs.b.Helper()

	// Start the mount process with format flag
	mfs.MountCmd = exec.CommandContext(mfs.ctx, mfs.BinaryPath,
		"-format",
		"-mount", mfs.MountPoint,
		"-device", mfs.ImagePath,
	)

	// Capture output for debugging
	var stdout, stderr strings.Builder
	mfs.MountCmd.Stdout = &stdout
	mfs.MountCmd.Stderr = &stderr

	if err := mfs.MountCmd.Start(); err != nil {
		mfs.b.Fatalf("Failed to start mount: %v\nStderr: %s", err, stderr.String())
	}

	// Wait for mount to be accessible
	if err := mfs.waitForMount(10 * time.Second); err != nil {
		mfs.MountCmd.Process.Kill()
		mfs.b.Fatalf("Mount not accessible: %v\nStdout: %s\nStderr: %s",
			err, stdout.String(), stderr.String())
	}
}

// waitForMount waits for the mount point to become accessible.
func (mfs *BenchmarkFS) waitForMount(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		// Try to list the directory
		entries, err := os.ReadDir(mfs.MountPoint)
		if err == nil {
			_ = entries // Mount is accessible
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for mount")
}

// Cleanup unmounts and removes the test filesystem.
func (mfs *BenchmarkFS) Cleanup() {
	mfs.b.Helper()

	// Stop the mount process
	mfs.cancel()
	if mfs.MountCmd != nil && mfs.MountCmd.Process != nil {
		mfs.MountCmd.Process.Signal(os.Interrupt)
		time.Sleep(100 * time.Millisecond)
	}

	// Try fusermount -u first
	unmount := exec.Command("fusermount", "-u", mfs.MountPoint)
	unmount.Run()

	// Give it time to unmount
	time.Sleep(100 * time.Millisecond)

	// Clean up files
	os.RemoveAll(mfs.MountPoint)
	os.Remove(mfs.ImagePath)
}

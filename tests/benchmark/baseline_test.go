// Package benchmark provides performance benchmarks for GoClusterFS.
// Run with: go test -bench=. -benchmem ./tests/benchmark/
package benchmark

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// BenchmarkSequentialRead measures sequential read throughput.
func BenchmarkSequentialRead(b *testing.B) {
	mfs := setupBenchmarkFS(b, 100) // 100MB test image
	defer mfs.Cleanup()

	// Create a 10MB file
	data := make([]byte, 10*1024*1024)
	rand.Read(data)
	filePath := filepath.Join(mfs.MountPoint, "benchmark_read.dat")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	buf := make([]byte, 64*1024) // 64KB read buffer

	b.ResetTimer()
	b.SetBytes(int64(len(buf)))

	for i := 0; i < b.N; i++ {
		f, err := os.Open(filePath)
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		offset := int64((i * len(buf)) % len(data))
		_, err = f.ReadAt(buf, offset)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
		f.Close()
	}
}

// BenchmarkSequentialWrite measures sequential write throughput.
func BenchmarkSequentialWrite(b *testing.B) {
	mfs := setupBenchmarkFS(b, 100) // 100MB test image
	defer mfs.Cleanup()

	data := make([]byte, 64*1024) // 64KB write buffer
	rand.Read(data)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		filePath := filepath.Join(mfs.MountPoint, fmt.Sprintf("benchmark_write_%d.dat", i%100))
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			b.Fatalf("Failed to create file: %v", err)
		}

		_, err = f.Write(data)
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}
		f.Close()
	}
}

// BenchmarkRandomRead measures random 4KB read latency.
func BenchmarkRandomRead(b *testing.B) {
	mfs := setupBenchmarkFS(b, 100) // 100MB test image
	defer mfs.Cleanup()

	// Create a 10MB file
	data := make([]byte, 10*1024*1024)
	rand.Read(data)
	filePath := filepath.Join(mfs.MountPoint, "benchmark_random_read.dat")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	buf := make([]byte, 4096) // 4KB read buffer

	b.ResetTimer()
	b.SetBytes(4096)

	for i := 0; i < b.N; i++ {
		f, err := os.Open(filePath)
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		// Random offset aligned to 4KB
		offset := int64((i * 4096 * 17) % (len(data) - 4096))
		offset = offset &^ 4095 // Align to 4KB
		_, err = f.ReadAt(buf, offset)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
		f.Close()
	}
}

// BenchmarkRandomWrite measures random 4KB write latency.
func BenchmarkRandomWrite(b *testing.B) {
	mfs := setupBenchmarkFS(b, 100) // 100MB test image
	defer mfs.Cleanup()

	// Create a 10MB file
	data := make([]byte, 10*1024*1024)
	filePath := filepath.Join(mfs.MountPoint, "benchmark_random_write.dat")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	buf := make([]byte, 4096) // 4KB write buffer
	rand.Read(buf)

	b.ResetTimer()
	b.SetBytes(4096)

	for i := 0; i < b.N; i++ {
		f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}

		// Random offset aligned to 4KB
		offset := int64((i * 4096 * 17) % (len(data) - 4096))
		offset = offset &^ 4095 // Align to 4KB
		_, err = f.WriteAt(buf, offset)
		if err != nil {
			b.Fatalf("Failed to write: %v", err)
		}
		f.Close()
	}
}

// BenchmarkConcurrentReads measures parallel read performance with N goroutines.
func BenchmarkConcurrentReads(b *testing.B) {
	for _, numReaders := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("readers=%d", numReaders), func(b *testing.B) {
			mfs := setupBenchmarkFS(b, 100) // 100MB test image
			defer mfs.Cleanup()

			// Create test files - one per reader
			fileSize := 1024 * 1024 // 1MB each
			data := make([]byte, fileSize)
			rand.Read(data)

			filePaths := make([]string, numReaders)
			for i := 0; i < numReaders; i++ {
				filePaths[i] = filepath.Join(mfs.MountPoint, fmt.Sprintf("concurrent_read_%d.dat", i))
				if err := os.WriteFile(filePaths[i], data, 0644); err != nil {
					b.Fatalf("Failed to create test file: %v", err)
				}
			}

			buf := make([]byte, 64*1024) // 64KB per read

			b.ResetTimer()
			b.SetBytes(int64(numReaders * len(buf)))

			var wg sync.WaitGroup
			opsPerReader := b.N / numReaders
			if opsPerReader == 0 {
				opsPerReader = 1
			}

			for r := 0; r < numReaders; r++ {
				wg.Add(1)
				go func(readerID int) {
					defer wg.Done()
					localBuf := make([]byte, len(buf))

					for i := 0; i < opsPerReader; i++ {
						f, err := os.Open(filePaths[readerID])
						if err != nil {
							return
						}
						offset := int64((i * len(localBuf)) % (fileSize - len(localBuf)))
						f.ReadAt(localBuf, offset)
						f.Close()
					}
				}(r)
			}
			wg.Wait()
		})
	}
}

// BenchmarkConcurrentWrites measures parallel write performance with N goroutines.
func BenchmarkConcurrentWrites(b *testing.B) {
	for _, numWriters := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("writers=%d", numWriters), func(b *testing.B) {
			mfs := setupBenchmarkFS(b, 100) // 100MB test image
			defer mfs.Cleanup()

			data := make([]byte, 64*1024) // 64KB write buffer
			rand.Read(data)

			b.ResetTimer()
			b.SetBytes(int64(numWriters * len(data)))

			var wg sync.WaitGroup
			opsPerWriter := b.N / numWriters
			if opsPerWriter == 0 {
				opsPerWriter = 1
			}

			for w := 0; w < numWriters; w++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()
					localData := make([]byte, len(data))
					copy(localData, data)

					for i := 0; i < opsPerWriter; i++ {
						filePath := filepath.Join(mfs.MountPoint,
							fmt.Sprintf("concurrent_write_%d_%d.dat", writerID, i%10))
						f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
						if err != nil {
							return
						}
						f.Write(localData)
						f.Close()
					}
				}(w)
			}
			wg.Wait()
		})
	}
}

// BenchmarkCacheHitRate measures performance of repeated reads to the same file.
func BenchmarkCacheHitRate(b *testing.B) {
	mfs := setupBenchmarkFS(b, 50) // 50MB test image
	defer mfs.Cleanup()

	// Create a small file that should fit in cache
	data := make([]byte, 64*1024) // 64KB
	rand.Read(data)
	filePath := filepath.Join(mfs.MountPoint, "cache_test.dat")
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	buf := make([]byte, len(data))

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		f, err := os.Open(filePath)
		if err != nil {
			b.Fatalf("Failed to open file: %v", err)
		}
		_, err = f.Read(buf)
		if err != nil {
			b.Fatalf("Failed to read: %v", err)
		}
		if !bytes.Equal(buf, data) {
			b.Fatalf("Data mismatch")
		}
		f.Close()
	}
}

// BenchmarkMixedWorkload measures performance of a 70/30 read/write mix.
func BenchmarkMixedWorkload(b *testing.B) {
	mfs := setupBenchmarkFS(b, 100) // 100MB test image
	defer mfs.Cleanup()

	// Create initial files
	fileSize := 256 * 1024 // 256KB each
	data := make([]byte, fileSize)
	rand.Read(data)

	numFiles := 20
	for i := 0; i < numFiles; i++ {
		filePath := filepath.Join(mfs.MountPoint, fmt.Sprintf("mixed_%d.dat", i))
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}
	}

	buf := make([]byte, 64*1024)
	rand.Read(buf)

	b.ResetTimer()
	b.SetBytes(int64(len(buf)))

	for i := 0; i < b.N; i++ {
		fileNum := i % numFiles
		filePath := filepath.Join(mfs.MountPoint, fmt.Sprintf("mixed_%d.dat", fileNum))

		if i%10 < 7 { // 70% reads
			f, err := os.Open(filePath)
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}
			f.Read(buf)
			f.Close()
		} else { // 30% writes
			f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
			if err != nil {
				b.Fatalf("Failed to open file: %v", err)
			}
			f.Write(buf)
			f.Close()
		}
	}
}

// BenchmarkFileCreation measures file creation overhead.
func BenchmarkFileCreation(b *testing.B) {
	mfs := setupBenchmarkFS(b, 100) // 100MB test image
	defer mfs.Cleanup()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		filePath := filepath.Join(mfs.MountPoint, fmt.Sprintf("create_%d.dat", i))
		f, err := os.Create(filePath)
		if err != nil {
			b.Fatalf("Failed to create file: %v", err)
		}
		f.Close()
	}
}

// BenchmarkDirectoryListing measures readdir performance.
func BenchmarkDirectoryListing(b *testing.B) {
	mfs := setupBenchmarkFS(b, 100) // 100MB test image
	defer mfs.Cleanup()

	// Create many files in a directory
	for i := 0; i < 100; i++ {
		filePath := filepath.Join(mfs.MountPoint, fmt.Sprintf("dir_list_%d.dat", i))
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			b.Fatalf("Failed to create test file: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entries, err := os.ReadDir(mfs.MountPoint)
		if err != nil {
			b.Fatalf("Failed to read directory: %v", err)
		}
		if len(entries) < 100 {
			b.Fatalf("Expected at least 100 entries, got %d", len(entries))
		}
	}
}

// BenchmarkMetadataOperations measures stat/chmod/chown performance.
func BenchmarkMetadataOperations(b *testing.B) {
	mfs := setupBenchmarkFS(b, 50) // 50MB test image
	defer mfs.Cleanup()

	filePath := filepath.Join(mfs.MountPoint, "metadata_test.dat")
	if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
		b.Fatalf("Failed to create test file: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := os.Stat(filePath)
		if err != nil {
			b.Fatalf("Failed to stat file: %v", err)
		}
	}
}

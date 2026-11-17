package integration

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const tearFileSize = 1 << 20

// fillWords returns tearFileSize bytes whose every 8-byte word is v.
func fillWords(v uint64) []byte {
	b := make([]byte, tearFileSize)
	for off := 0; off < len(b); off += 8 {
		binary.LittleEndian.PutUint64(b[off:], v)
	}
	return b
}

// mixedAt returns the offset of the first word in b that differs from the first one, or -1.
func mixedAt(b []byte) int {
	first := binary.LittleEndian.Uint64(b)
	for off := 8; off < len(b); off += 8 {
		if binary.LittleEndian.Uint64(b[off:]) != first {
			return off
		}
	}
	return -1
}

// readUntil reads path whole, in one request per read, until stop closes,
// counts its reads in reads, and describes each one that mixed two writes.
func readUntil(path string, oDirect bool, stop <-chan struct{}, reads *atomic.Int64) ([]string, error) {
	flags := unix.O_RDONLY
	if oDirect {
		flags |= unix.O_DIRECT
	}
	fd, err := unix.Open(path, flags, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(fd) }()
	buf, err := unix.Mmap(-1, 0, tearFileSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Munmap(buf) }()
	var tears []string
	for {
		select {
		case <-stop:
			return tears, nil
		default:
		}
		n, err := unix.Pread(fd, buf, 0)
		if err != nil {
			return tears, err
		}
		if n != tearFileSize {
			return tears, fmt.Errorf("read %d bytes of %d", n, tearFileSize)
		}
		reads.Add(1)
		if at := mixedAt(buf); at >= 0 {
			tears = append(tears, fmt.Sprintf("write %d up to byte %d, then write %d",
				binary.LittleEndian.Uint64(buf), at, binary.LittleEndian.Uint64(buf[at:])))
		}
	}
}

// enoughReads waits until every node made minReads whole reads and minTime
// passed. After maxTime it fails, naming the node furthest behind.
func enoughReads(reads []atomic.Int64, minTime, maxTime time.Duration) error {
	const minReads = 10
	start := time.Now()
	for {
		slowest := 0
		for i := range reads {
			if reads[i].Load() < reads[slowest].Load() {
				slowest = i
			}
		}
		n := reads[slowest].Load()
		switch {
		case n >= minReads && time.Since(start) >= minTime:
			return nil
		case time.Since(start) >= maxTime:
			return fmt.Errorf("node %d made only %d whole reads in %v while node 0 wrote, want %d", slowest, n, maxTime, minReads)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// One node keeps overwriting a 1 MB file, each write one value throughout,
// while every node reads it whole: no read may return parts of two writes.
func TestMultiNodeReadsNeverSeePartOfAWrite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		oDirect bool
	}{
		{"O_DIRECT reads", nil, true},
		{"mounted with -direct-io", []string{"-direct-io"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if testing.Short() && !tc.oDirect {
				t.Skip("one mode is enough with -short")
			}
			duration := 8 * time.Second
			if testing.Short() {
				duration = 3 * time.Second
			}
			cluster := NewMultiNodeCluster(t, 3, 64)
			for i := range cluster.NodeCount() {
				cluster.Node(i).ExtraArgs = append(cluster.Node(i).ExtraArgs, tc.args...)
			}
			if err := cluster.FormatAndStartFirst(false); err != nil {
				t.Fatal(err)
			}
			if err := cluster.StartAllNodes(false); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitAllMounted(30 * time.Second); err != nil {
				t.Fatal(err)
			}
			if err := cluster.Node(0).WriteFile("tear", fillWords(0)); err != nil {
				t.Fatal(err)
			}
			for i := 1; i < cluster.NodeCount(); i++ {
				if _, ok := waitFor(10*time.Second, func() bool {
					info, err := cluster.Node(i).Stat("tear")
					return err == nil && info.Size() == tearFileSize
				}); !ok {
					t.Fatalf("node %d never saw the 1 MB file", i)
				}
			}

			stop := make(chan struct{})
			var writes atomic.Int64
			var wg sync.WaitGroup
			wg.Go(func() {
				f, err := os.OpenFile(cluster.Node(0).Path("tear"), os.O_WRONLY, 0)
				if err != nil {
					t.Error(err)
					return
				}
				defer mustClose(t, f)
				for v := uint64(1); ; v++ {
					select {
					case <-stop:
						return
					default:
					}
					if _, err := f.WriteAt(fillWords(v), 0); err != nil {
						t.Errorf("node 0 write %d: %v", v, err)
						return
					}
					writes.Add(1)
				}
			})
			reads := make([]atomic.Int64, cluster.NodeCount())
			tears := make([][]string, cluster.NodeCount())
			for i := range cluster.NodeCount() {
				wg.Go(func() {
					var err error
					tears[i], err = readUntil(cluster.Node(i).Path("tear"), tc.oDirect, stop, &reads[i])
					if err != nil {
						t.Errorf("node %d read: %v", i, err)
					}
				})
			}
			if err := enoughReads(reads, duration, time.Minute); err != nil {
				t.Error(err)
			}
			close(stop)
			wg.Wait()

			counts := make([]int64, len(reads))
			for i := range reads {
				counts[i] = reads[i].Load()
			}
			t.Logf("%d writes on node 0; reads per node %v", writes.Load(), counts)
			if writes.Load() < 10 {
				t.Errorf("node 0 made only %d writes while the other nodes read, want at least 10", writes.Load())
			}
			for i, torn := range tears {
				if len(torn) > 0 {
					t.Errorf("node %d: %d of %d reads mixed two writes, first: %s", i, len(torn), counts[i], torn[0])
				}
			}
		})
	}
}

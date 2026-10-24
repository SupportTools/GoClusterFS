package integration

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"syscall"
	"testing"
)

// Parallel first writes through one mount into one fallocated block must keep
// every writer's data.
func TestConcurrentPwritesToFallocatedBlock(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	const writers, rounds, size = 16, 20, 4096
	for round := 0; round < rounds; round++ {
		name := fmt.Sprintf("prealloc-%d.bin", round)
		f, err := os.OpenFile(mfs.Path(name), os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			t.Fatalf("OpenFile failed: %v", err)
		}
		if err := syscall.Fallocate(int(f.Fd()), 0, 0, 1<<20); err != nil {
			f.Close()
			t.Fatalf("Fallocate failed: %v", err)
		}

		start := make(chan struct{})
		errs := make(chan error, writers)
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, err := f.WriteAt(bytes.Repeat([]byte{byte(w + 1)}, size), int64(w)<<16)
				errs <- err
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: WriteAt failed: %v", round, err)
			}
		}

		got := make([]byte, size)
		for w := 0; w < writers; w++ {
			if _, err := f.ReadAt(got, int64(w)<<16); err != nil {
				t.Fatalf("round %d: ReadAt failed: %v", round, err)
			}
			if !bytes.Equal(got, bytes.Repeat([]byte{byte(w + 1)}, size)) {
				t.Fatalf("round %d: writer %d's data was lost", round, w)
			}
		}
		f.Close()
		if err := os.Remove(mfs.Path(name)); err != nil {
			t.Fatalf("Remove failed: %v", err)
		}
	}
}

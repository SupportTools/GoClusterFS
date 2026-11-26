package integration

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// Hundreds of creators, each in its own directory, fill the journal with open intents faster than they complete. A
// create then waits for journal space instead of failing with EIO.
func TestParallelCreatorsWaitForJournalSpace(t *testing.T) {
	creators := envInt("GOCLUSTERFS_TEST_CREATORS", 256)
	seconds := 20
	if testing.Short() {
		seconds = 10
	}
	run := time.Duration(envInt("GOCLUSTERFS_TEST_CREATE_SECONDS", seconds)) * time.Second
	mfs := NewMountedFS(t, envInt("GOCLUSTERFS_TEST_IMAGE_MB", 2048))
	mfs.ExtraArgs = append(mfs.ExtraArgs, "-bytes-per-inode", "65536")
	log := &syncBuffer{}
	mfs.Log = log
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	var created, failed atomic.Int64
	var mu sync.Mutex
	errnos := map[string]int{}
	deadline := time.Now().Add(run)
	var wg sync.WaitGroup
	for c := range creators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dir := mfs.Path(fmt.Sprintf("creator-%d", c))
			if err := os.Mkdir(dir, 0o755); err != nil {
				failed.Add(1)
				mu.Lock()
				errnos["mkdir: "+errnoName(err)]++
				mu.Unlock()
				return
			}
			for i := 0; time.Now().Before(deadline); i++ {
				f, err := os.OpenFile(fmt.Sprintf("%s/f-%d", dir, i), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
				if err != nil {
					failed.Add(1)
					mu.Lock()
					errnos[errnoName(err)]++
					mu.Unlock()
					continue
				}
				created.Add(1)
				if err := f.Close(); err != nil {
					t.Errorf("close: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	full := strings.Count(log.String(), "journal is full")
	t.Logf("%d creators for %v: %d files created, %d creates failed %v, %d \"journal is full\" log lines", creators, run, created.Load(), failed.Load(), errnos, full)
	if failed.Load() > 0 || full > 0 {
		t.Errorf("creates failed: %v; \"journal is full\" logged %d times", errnos, full)
	}
}

func errnoName(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	return err.Error()
}

// Hundreds of writers overwrite their own files and rename them, while open intents fill the journal. A write or a
// rename then waits for journal space instead of failing with EIO.
func TestParallelWritersWaitForJournalSpace(t *testing.T) {
	writers := envInt("GOCLUSTERFS_TEST_WRITERS", 256)
	seconds := 20
	if testing.Short() {
		seconds = 10
	}
	run := time.Duration(envInt("GOCLUSTERFS_TEST_WRITE_SECONDS", seconds)) * time.Second
	mfs := NewMountedFS(t, envInt("GOCLUSTERFS_TEST_IMAGE_MB", 8192))
	log := &syncBuffer{}
	mfs.Log = log
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount failed: %v", err)
	}

	var writes, renames, failed atomic.Int64
	var mu sync.Mutex
	errnos := map[string]int{}
	fail := func(op string, err error) {
		failed.Add(1)
		mu.Lock()
		errnos[op+": "+errnoName(err)]++
		mu.Unlock()
	}
	deadline := time.Now().Add(run)
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dir := mfs.Path(fmt.Sprintf("writer-%d", w))
			if err := os.Mkdir(dir, 0o755); err != nil {
				fail("mkdir", err)
				return
			}
			name := dir + "/f-0"
			buf := make([]byte, 256<<10)
			for i := range buf {
				buf[i] = byte(w + i)
			}
			for i := 1; time.Now().Before(deadline); i++ {
				f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0o644)
				if err != nil {
					fail("open", err)
					continue
				}
				if _, err := f.WriteAt(buf, int64(i%8)*int64(len(buf))); err != nil {
					fail("write", err)
				} else {
					writes.Add(1)
				}
				if err := f.Close(); err != nil {
					fail("close", err)
				}
				next := fmt.Sprintf("%s/f-%d", dir, i)
				if err := os.Rename(name, next); err != nil {
					fail("rename", err)
					continue
				}
				name = next
				renames.Add(1)
			}
		}()
	}
	wg.Wait()
	full := strings.Count(log.String(), "journal is full")
	t.Logf("%d writers for %v: %d writes, %d renames, %d failed %v, %d \"journal is full\" log lines", writers, run, writes.Load(), renames.Load(), failed.Load(), errnos, full)
	if failed.Load() > 0 || full > 0 {
		t.Errorf("operations failed: %v; \"journal is full\" logged %d times", errnos, full)
	}
}

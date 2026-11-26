package integration

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	claimCollisionRe = regexp.MustCompile(`inode slot collision, retry`)
	mountProblemRe   = regexp.MustCompile(`WARN|ERROR|failed|TIMEOUT|concurrently`)
)

// TestMultiNodeConcurrentCreatesDoNotCollide has two nodes create files in one
// directory at the same time. Each node starts its inode search in its own part
// of the table, so no create fails with EAGAIN and inode claims do not collide,
// and each node then reads every file back from the other node.
func TestMultiNodeConcurrentCreatesDoNotCollide(t *testing.T) {
	perNode := 150
	if testing.Short() {
		perNode = 30
	}
	cluster := NewMultiNodeCluster(t, 2, 2*perNode+50)
	logs := []*syncBuffer{{}, {}}
	cluster.Node(0).Log, cluster.Node(1).Log = logs[0], logs[1]
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node 1: %v", err)
	}

	start := time.Now()
	errs := cluster.RunConcurrent([]int{0, 1}, func(idx int, node *MountedFS) error {
		eagain, failed := 0, 0
		var firstErr error
		for i := 0; i < perNode; i++ {
			name := fmt.Sprintf("node%d-%03d", idx, i)
			err := node.WriteFile(name, []byte(name))
			switch {
			case errors.Is(err, syscall.EAGAIN):
				eagain++
			case err != nil:
				failed++
			}
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if eagain+failed > 0 {
			return fmt.Errorf("%d creates failed with EAGAIN and %d with other errors (first: %v)", eagain, failed, firstErr)
		}
		return nil
	})
	elapsed := time.Since(start)
	collisions := len(claimCollisionRe.FindAllString(logs[0].String(), -1)) +
		len(claimCollisionRe.FindAllString(logs[1].String(), -1))
	t.Logf("%d concurrent creates on 2 nodes took %v with %d inode claim collisions",
		2*perNode, elapsed, collisions)

	for idx, err := range errs {
		if err != nil {
			t.Errorf("node %d: %v", idx, err)
		}
	}
	if collisions > 0 {
		t.Errorf("%d inode claims collided between the nodes, want 0", collisions)
	}

	for reader := 0; reader < 2; reader++ {
		writer := 1 - reader
		var missing []string
		var readErr error
		_, ok := waitFor(propagationTimeout, func() bool {
			missing = missing[:0]
			for i := 0; i < perNode; i++ {
				name := fmt.Sprintf("node%d-%03d", writer, i)
				data, err := cluster.Node(reader).ReadFile(name)
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					readErr = err
					return true
				}
				if err != nil || string(data) != name {
					missing = append(missing, name)
				}
			}
			return len(missing) == 0
		})
		switch {
		case readErr != nil:
			t.Errorf("node %d reading node %d's files: %v", reader, writer, readErr)
		case !ok:
			t.Errorf("node %d cannot read %d of node %d's files: %v", reader, len(missing), writer, missing)
		}
	}
	if t.Failed() {
		for idx, buf := range logs {
			for _, line := range strings.Split(buf.String(), "\n") {
				if mountProblemRe.MatchString(line) {
					t.Logf("node %d: %s", idx, line)
				}
			}
		}
	}
}

var lostRacesRe = regexp.MustCompile(`lost \d+ races with other nodes`)

// TestLocalConcurrentCreatesGetTheirOwnSlots has 16 creators on one node create files in their own directories at
// once. A create finds a free inode slot, then commits to the journal before it claims the slot. When every creator
// found the same slot, the losers kept colliding until creates failed with EAGAIN and the mount logged "lost 5 races
// with other nodes", with no other node mounted.
func TestLocalConcurrentCreatesGetTheirOwnSlots(t *testing.T) {
	const creators = 16
	perCreator := 100
	if testing.Short() {
		perCreator = 40
	}
	mfs := NewMountedFS(t, 1024)
	var log syncBuffer
	mfs.Log = &log
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("format and mount: %v", err)
	}
	for c := range creators {
		if err := os.Mkdir(filepath.Join(mfs.MountPoint, fmt.Sprintf("c%02d", c)), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	failed := make([]int, creators)
	firstErr := make([]error, creators)
	var wg sync.WaitGroup
	start := time.Now()
	for c := range creators {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perCreator {
				name := filepath.Join(mfs.MountPoint, fmt.Sprintf("c%02d", c), fmt.Sprintf("f%03d", i))
				f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
				if err != nil {
					failed[c]++
					if firstErr[c] == nil {
						firstErr[c] = err
					}
					continue
				}
				_ = f.Close()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	total := creators * perCreator
	t.Logf("%d creators made %d files in %v: %.0f creates/s", creators, total, elapsed, float64(total)/elapsed.Seconds())

	for c := range creators {
		if failed[c] > 0 {
			t.Errorf("creator %d: %d of %d creates failed (first: %v)", c, failed[c], perCreator, firstErr[c])
		}
	}
	if lost := lostRacesRe.FindAllString(log.String(), -1); len(lost) > 0 {
		t.Errorf("the mount logged %d creates that lost every race with other nodes, with no other node mounted", len(lost))
	}
}

// TestCrossNodeCreatesInOnePartOfTheTable has node 0 create files until it runs into node 1's part of the inode table,
// then has both nodes create files with 16 creators each. Node 0's search then followed right behind node 1's and kept
// finding the inode node 1 was about to claim, so creates failed with EAGAIN after 5 lost races.
func TestCrossNodeCreatesInOnePartOfTheTable(t *testing.T) {
	const creators = 16
	perCreator := 60
	if testing.Short() {
		perCreator = 30
	}
	cluster := NewMultiNodeCluster(t, 2, 1024)
	logs := []*syncBuffer{{}, {}}
	cluster.Node(0).Log, cluster.Node(1).Log = logs[0], logs[1]
	cluster.Node(0).ExtraArgs = append(cluster.Node(0).ExtraArgs, "-bytes-per-inode", "262144")
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	inodeOf := func(path string) uint64 {
		var st syscall.Stat_t
		if err := syscall.Stat(path, &st); err != nil {
			t.Fatal(err)
		}
		return st.Ino
	}
	n0, n1 := cluster.Node(0).MountPoint, cluster.Node(1).MountPoint
	if err := os.WriteFile(filepath.Join(n1, "probe"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	nodeOneStart := inodeOf(filepath.Join(n1, "probe"))
	if err := os.Mkdir(filepath.Join(n0, "fill"), 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		name := filepath.Join(n0, "fill", fmt.Sprintf("f%05d", i))
		if err := os.WriteFile(name, nil, 0o644); err != nil {
			t.Fatalf("filling node 0's part of the table: %v", err)
		}
		if inodeOf(name) > nodeOneStart {
			break
		}
	}
	for idx, mnt := range []string{n0, n1} {
		for c := range creators {
			if err := os.Mkdir(filepath.Join(mnt, fmt.Sprintf("n%dc%02d", idx, c)), 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}

	var mu sync.Mutex
	failed := map[string]int{}
	start := time.Now()
	cluster.RunConcurrent([]int{0, 1}, func(idx int, node *MountedFS) error {
		var wg sync.WaitGroup
		for c := range creators {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range perCreator {
					name := filepath.Join(node.MountPoint, fmt.Sprintf("n%dc%02d", idx, c), fmt.Sprintf("f%03d", i))
					f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
					if err != nil {
						mu.Lock()
						failed[fmt.Sprintf("node %d: %v", idx, errors.Unwrap(err))]++
						mu.Unlock()
						continue
					}
					_ = f.Close()
				}
			}()
		}
		wg.Wait()
		return nil
	})
	elapsed := time.Since(start)
	total := 2 * creators * perCreator
	t.Logf("2 nodes x %d creators made %d files in one part of the table in %v: %.0f creates/s", creators, total, elapsed, float64(total)/elapsed.Seconds())
	for kind, n := range failed {
		t.Errorf("%d creates failed with %s", n, kind)
	}
	for idx, buf := range logs {
		if lost := lostRacesRe.FindAllString(buf.String(), -1); len(lost) > 0 {
			t.Errorf("node %d logged %d creates that lost every race with the other node", idx, len(lost))
		}
	}
}

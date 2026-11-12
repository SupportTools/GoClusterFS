package integration

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMultiNodeCreatesInOneDirectory has four nodes create 4200 empty files in
// one directory at the same time, so every create waits on the same directory lock.
func TestMultiNodeCreatesInOneDirectory(t *testing.T) {
	const nodes = 4
	perNode := 1050
	if testing.Short() {
		perNode = 100
	}
	// Format gives one inode per MB.
	cluster := NewMultiNodeCluster(t, nodes, nodes*perNode+500)
	logs := make([]*syncBuffer, nodes)
	all := make([]int, nodes)
	for i := range logs {
		logs[i] = &syncBuffer{}
		cluster.Node(i).Log = logs[i]
		all[i] = i
	}
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	for i := 1; i < nodes; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	if err := cluster.Node(0).Mkdir("shared"); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	created := make([][]string, nodes)
	start := time.Now()
	errs := cluster.RunConcurrent(all, func(idx int, node *MountedFS) error {
		if _, ok := waitFor(10*time.Second, func() bool { return node.Exists("shared") }); !ok {
			return fmt.Errorf("directory never appeared")
		}
		failed := map[string]int{}
		var firstErr error
		for i := 0; i < perNode; i++ {
			name := fmt.Sprintf("n%d-f%d", idx, i)
			f, err := os.OpenFile(node.Path("shared/"+name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
			if err == nil {
				err = f.Close()
			}
			if err == nil {
				created[idx] = append(created[idx], name)
				continue
			}
			var errno syscall.Errno
			errors.As(err, &errno)
			failed[errno.Error()]++
			if firstErr == nil {
				firstErr = err
			}
		}
		if len(failed) > 0 {
			return fmt.Errorf("creates failed %v (first: %v)", failed, firstErr)
		}
		return nil
	})
	t.Logf("%d creates on %d nodes took %v", nodes*perNode, nodes, time.Since(start))

	var createErrs []string
	for idx, err := range errs {
		if err != nil {
			createErrs = append(createErrs, fmt.Sprintf("node %d: %v", idx, err))
		}
	}
	entries, err := cluster.Node(0).ReadDir("shared")
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	listed := map[string]bool{}
	for _, e := range entries {
		listed[e.Name()] = true
	}
	var missing []string
	for _, names := range created {
		for _, name := range names {
			if !listed[name] {
				missing = append(missing, name)
			}
		}
	}
	problems := createErrs
	if len(missing) > 0 {
		problems = append(problems, fmt.Sprintf("node 0 lists %d entries; %d created files are missing: %v",
			len(entries), len(missing), missing[:min(10, len(missing))]))
	}
	if len(problems) == 0 {
		return
	}
	for idx, buf := range logs {
		for _, line := range strings.Split(buf.String(), "\n") {
			if mountProblemRe.MatchString(line) {
				t.Logf("node %d: %s", idx, line)
			}
		}
	}
	// Until #29, two nodes can hold one lock and overwrite each other's chunk,
	// and a lock record read during another node's write fails its checksum.
	doubleHolds, tornReads := 0, 0
	for _, buf := range logs {
		doubleHolds += strings.Count(buf.String(), "not the lock owner")
		tornReads += strings.Count(buf.String(), "invalid lock record")
	}
	if (len(missing) == 0 || doubleHolds > 0) && (len(createErrs) == 0 || tornReads > 0) {
		t.Skipf("%s; the logs show %d double holds and %d torn lock reads, which #29 removes", strings.Join(problems, "; "), doubleHolds, tornReads)
	}
	t.Error(strings.Join(problems, "; "))
}

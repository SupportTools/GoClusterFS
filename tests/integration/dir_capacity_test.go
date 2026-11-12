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

	start := time.Now()
	errs := cluster.RunConcurrent(all, func(idx int, node *MountedFS) error {
		if _, ok := waitFor(10*time.Second, func() bool { return node.Exists("shared") }); !ok {
			return fmt.Errorf("directory never appeared")
		}
		failed := map[string]int{}
		var firstErr error
		for i := 0; i < perNode; i++ {
			f, err := os.OpenFile(node.Path(fmt.Sprintf("shared/n%d-f%d", idx, i)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
			if err == nil {
				err = f.Close()
			}
			if err != nil {
				var errno syscall.Errno
				errors.As(err, &errno)
				failed[errno.Error()]++
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		if len(failed) > 0 {
			return fmt.Errorf("creates failed %v (first: %v)", failed, firstErr)
		}
		return nil
	})
	t.Logf("%d creates on %d nodes took %v", nodes*perNode, nodes, time.Since(start))

	for idx, err := range errs {
		if err != nil {
			t.Errorf("node %d: %v", idx, err)
		}
	}
	entries, err := cluster.Node(0).ReadDir("shared")
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != nodes*perNode {
		t.Errorf("node 0 lists %d entries, want %d", len(entries), nodes*perNode)
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

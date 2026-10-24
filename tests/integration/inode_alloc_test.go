package integration

import (
	"errors"
	"fmt"
	"regexp"
	"syscall"
	"testing"
	"time"
)

var claimCollisionRe = regexp.MustCompile(`inode slot collision, retry`)

// TestMultiNodeConcurrentCreatesDoNotCollide has two nodes create files in one
// directory at the same time. Each node starts its inode search in its own part
// of the table, so no create fails with EAGAIN and inode claims do not collide.
// It does not read the files back from the other node: concurrent directory
// updates can still lose an entry until lock claims are confirmed (PR #29).
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
	t.Logf("%d concurrent creates on 2 nodes took %v with %d inode claim collisions", 2*perNode, elapsed, collisions)

	for idx, err := range errs {
		if err != nil {
			t.Errorf("node %d: %v", idx, err)
		}
	}
	if collisions > 0 {
		t.Errorf("%d inode claims collided between the nodes, want 0", collisions)
	}
}

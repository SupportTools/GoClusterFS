package integration

import (
	"context"
	"fmt"
	"regexp"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// TestSurvivorReleasesCrashedNodeLocks crashes node A while it holds the root
// directory lock. Node B must prove A dead and take the lock well before the
// 15s staleness threshold, within a 10s mount timeout.
func TestSurvivorReleasesCrashedNodeLocks(t *testing.T) {
	cluster, uuidA, logB := startPairForLockTest(t)

	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("kill node A: %v", err)
	}
	release := holdRootLockAs(t, cluster.ImagePath, uuidA)
	defer release()

	start := time.Now()
	for {
		err := cluster.Node(1).WriteFile("after-crash", []byte("after-crash"))
		if err == nil {
			break
		}
		if time.Since(start) > 10*time.Second {
			t.Fatalf("node B could not create a file within 10s of node A's crash: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("node B needed %v to create a file after node A's crash, want under 10s", took)
	} else {
		t.Logf("node B created a file %v after node A crashed holding the root directory lock", took)
	}

	reclaimed := regexp.MustCompile(`reclaimed \d+ lock records of dead node ` + fmt.Sprintf("%x", uuidA[:8]))
	if !reclaimed.MatchString(logB.String()) {
		t.Error("node B did not log releasing node A's locks")
	}
}

// TestSlowNodeKeepsLocks pauses node A (alive, but its heartbeat stops) while
// it holds the root directory lock. Node B must not take the lock while A's
// process exists and A's heartbeat has not been silent for the heartbeat
// timeout plus margin by B's own clock.
func TestSlowNodeKeepsLocks(t *testing.T) {
	cluster, uuidA, logB := startPairForLockTest(t)
	pid := cluster.Node(0).PID()

	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("pause node A: %v", err)
	}
	defer syscall.Kill(pid, syscall.SIGCONT)
	release := holdRootLockAs(t, cluster.ImagePath, uuidA)

	attempt := func(result chan<- error) {
		result <- cluster.Node(1).WriteFile("while-a-paused", []byte("x"))
	}
	result := make(chan error, 1)
	go attempt(result)
	paused := time.After(12 * time.Second)
	for waiting := true; waiting; {
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("node B took the root directory lock of a node that is paused but alive")
			}
			time.Sleep(500 * time.Millisecond)
			go attempt(result)
		case <-paused:
			waiting = false
		}
	}
	if regexp.MustCompile(`lock records of dead node ` + fmt.Sprintf("%x", uuidA[:8])).MatchString(logB.String()) {
		t.Error("node B released the locks of a paused, live node")
	}

	syscall.Kill(pid, syscall.SIGCONT)
	release()
	if err := cluster.Node(1).WriteFile("after-resume", []byte("x")); err != nil {
		t.Errorf("node B could not create a file once node A resumed and released the lock: %v", err)
	}
}

func startPairForLockTest(t *testing.T) (*MultiNodeCluster, [16]byte, *syncBuffer) {
	t.Helper()
	cluster := NewMultiNodeCluster(t, 2, 50)
	logA, logB := &syncBuffer{}, &syncBuffer{}
	cluster.Node(0).Log, cluster.Node(1).Log = logA, logB
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node A: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node B: %v", err)
	}
	return cluster, nodeUUID(t, cluster.ImagePath, mustMatch(t, registeredRe, logA, "node A registration")), logB
}

func nodeUUID(t *testing.T, imagePath, prefix string) [16]byte {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	slots, err := node.NewRegistry(dev, sb.NodeRegistryOffset).GetActiveNodes()
	if err != nil {
		t.Fatal(err)
	}
	for _, slot := range slots {
		if fmt.Sprintf("%x", slot.UUID[:8]) == prefix {
			return slot.UUID
		}
	}
	t.Fatalf("no active registry slot for node %s", prefix)
	return [16]byte{}
}

// holdRootLockAs writes an exclusive root directory lock record owned by owner, as if owner held it.
func holdRootLockAs(t *testing.T, imagePath string, owner [16]byte) func() {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(dev)
	if err != nil {
		dev.Close()
		t.Fatal(err)
	}
	locks := lock.NewManager(dev, sb.LockBitmapOffset, sb.LockBitmapSize, owner, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := locks.Acquire(ctx, lock.LockTypeInode, uint64(inode.InodeRoot), lock.LockModeExclusive); err != nil {
		dev.Close()
		t.Fatalf("hold root directory lock as %x: %v", owner[:8], err)
	}
	return func() {
		locks.Release(lock.LockTypeInode, uint64(inode.InodeRoot))
		dev.Close()
	}
}

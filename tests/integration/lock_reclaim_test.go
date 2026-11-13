package integration

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
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
	cluster, nodeA, logB := startPairForLockTest(t)
	uuidA := nodeA.uuid

	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("kill node A: %v", err)
	}
	release := holdRootLockAs(t, cluster.ImagePath, nodeA)
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

	tookOver := regexp.MustCompile(`took over \d+ lock records of dead node ` + fmt.Sprintf("%x", uuidA[:8]))
	if !tookOver.MatchString(logB.String()) {
		t.Error("node B did not log taking over node A's locks")
	}
}

// TestSlowNodeKeepsLocks pauses node A for 17s while it holds the root
// directory lock: longer than the heartbeat timeout and B's own-clock check
// (15s), shorter than the 20s after which failure detection treats a silent
// node as dead. B must not take the lock before it goes stale, and must never
// take over A's locks as a dead node's.
func TestSlowNodeKeepsLocks(t *testing.T) {
	cluster, nodeA, logB := startPairForLockTest(t)
	uuidA := nodeA.uuid
	pid := cluster.Node(0).PID()

	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("pause node A: %v", err)
	}
	defer func() { _ = syscall.Kill(pid, syscall.SIGCONT) }()
	release := holdRootLockAs(t, cluster.ImagePath, nodeA)

	attempt := func(result chan<- error) {
		result <- cluster.Node(1).WriteFile("while-a-paused", []byte("x"))
	}
	result := make(chan error, 1)
	go attempt(result)
	paused := time.After(12 * time.Second)
	resume := time.After(17 * time.Second)
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
	<-resume
	if regexp.MustCompile(`lock records of dead node ` + fmt.Sprintf("%x", uuidA[:8])).MatchString(logB.String()) {
		t.Error("node B took over the locks of a paused, live node")
	}

	if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	release()
	if err := cluster.Node(1).WriteFile("after-resume", []byte("x")); err != nil {
		t.Errorf("node B could not create a file once node A resumed and released the lock: %v", err)
	}
}

// TestWaiterOutlastsLockTimeoutThroughFailover has node A hold the root
// directory lock while alive for 13s and then stop. Node B's create waits on
// that lock: for a live holder until A's heartbeat is 10s old, then for a
// failing one until A is proven dead and replayed. The wait runs past the 30s
// lock timeout, and B's create must still succeed.
func TestWaiterOutlastsLockTimeoutThroughFailover(t *testing.T) {
	cluster, nodeA, logB := startPairForLockTest(t)
	pid := cluster.Node(0).PID()
	release := holdRootLockAs(t, cluster.ImagePath, nodeA)
	defer release()

	start := time.Now()
	result := make(chan error, 1)
	go func() { result <- cluster.Node(1).WriteFile("through-failover", []byte("x")) }()
	time.Sleep(13 * time.Second)
	select {
	case err := <-result:
		t.Fatalf("node B's create returned (%v) while node A was alive and held the lock", err)
	default:
	}
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("stop node A: %v", err)
	}
	defer func() { _ = cluster.KillNode(0) }()

	select {
	case err := <-result:
		took := time.Since(start)
		if err != nil {
			t.Fatalf("node B's create failed after %v while node A failed over: %v", took, err)
		}
		if took < 30*time.Second {
			t.Errorf("node B's create took %v; the test needs a wait longer than the 30s lock timeout", took)
		}
		t.Logf("node B's create succeeded after waiting %v", took)
	case <-time.After(90 * time.Second):
		t.Fatal("node B's create was still waiting 103s after it started")
	}

	waited := fmt.Sprintf(`node %x \(slot %d\), which holds it and is failing or being recovered`, nodeA.uuid[:8], nodeA.slot)
	if !regexp.MustCompile(waited).MatchString(logB.String()) {
		t.Error("node B did not log that it waited for node A's recovery")
	}
	if !regexp.MustCompile(`took over \d+ lock records of dead node ` + fmt.Sprintf("%x", nodeA.uuid[:8])).MatchString(logB.String()) {
		t.Error("node B did not take over node A's locks")
	}
}

// TestStalledNodesWritesStopAtWriteGate stops node A while a writer keeps
// writing a file through it, waits until node B has proven A dead and replayed
// it, then writes the file from B and resumes A. A's heartbeat is held back after
// the pause, so A's queued write reaches the device first, where the write gate
// must refuse it: A exits, and B's data stays.
func TestStalledNodesWritesStopAtWriteGate(t *testing.T) {
	cluster, nodeA, _ := startPairForLockTestWithEnv(t, []string{"GOCLUSTERFS_TEST_HEARTBEAT_HOLD=30s"})
	a, b := cluster.Node(0), cluster.Node(1)
	logA := a.Log.(*syncBuffer)
	pid := a.PID()
	if err := b.WriteFile("fenced", bytes.Repeat([]byte{'0'}, 4096)); err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if a.WriteFileAt("fenced", bytes.Repeat([]byte{'A'}, 4096), 0) != nil {
				return
			}
		}
	}()
	time.Sleep(time.Second)
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("stop node A: %v", err)
	}
	defer cluster.KillNode(0)

	waitSlotEmpty(t, cluster.ImagePath, nodeA.slot, 90*time.Second)
	if err := b.WriteFileAt("fenced", bytes.Repeat([]byte{'B'}, 4096), 0); err != nil {
		t.Fatalf("node B writes after taking over: %v", err)
	}
	if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
		t.Fatalf("resume node A: %v", err)
	}

	// The held heartbeat stops A after 30s whatever the gate does; by then a write the gate let through has landed.
	lost := regexp.MustCompile(fmt.Sprintf(`Node registry slot %d is no longer this node's`, nodeA.slot))
	deadline := time.Now().Add(45 * time.Second)
	for !lost.MatchString(logA.String()) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	got, err := b.ReadFile("fenced")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{'B'}, 4096)) {
		t.Fatalf("file reads %q... after node A resumed, want node B's data", got[:16])
	}
	if !lost.MatchString(logA.String()) {
		t.Fatal("node A did not stop after resuming from its takeover")
	}
	if !strings.Contains(logA.String(), "checking the slot before a write") {
		t.Error("node A stopped without its write gate checking the slot")
	}
}

// waitSlotEmpty waits until registry slot slot is empty: a peer replayed its node.
func waitSlotEmpty(t *testing.T, imagePath string, slot int, timeout time.Duration) {
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
	reg := node.NewRegistry(dev, sb.NodeRegistryOffset)
	deadline := time.Now().Add(timeout)
	for {
		if _, state := reg.Occupant(slot, true); state == node.Unoccupied {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("registry slot %d still occupied after %v", slot, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

type registeredNode struct {
	uuid [16]byte
	slot int
}

func startPairForLockTest(t *testing.T) (*MultiNodeCluster, registeredNode, *syncBuffer) {
	t.Helper()
	return startPairForLockTestWithEnv(t, nil)
}

// startPairForLockTestWithEnv is startPairForLockTest with envA added to node A's environment.
func startPairForLockTestWithEnv(t *testing.T, envA []string) (*MultiNodeCluster, registeredNode, *syncBuffer) {
	t.Helper()
	cluster := NewMultiNodeCluster(t, 2, 50)
	cluster.Node(0).Env = envA
	logA, logB := &syncBuffer{}, &syncBuffer{}
	cluster.Node(0).Log, cluster.Node(1).Log = logA, logB
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node A: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node B: %v", err)
	}
	slot, err := strconv.Atoi(mustMatch(t, slotRe, logA, "node A registration"))
	if err != nil {
		t.Fatal(err)
	}
	return cluster, registeredNode{nodeUUID(t, cluster.ImagePath, mustMatch(t, registeredRe, logA, "node A registration")), slot}, logB
}

func nodeUUID(t *testing.T, imagePath, prefix string) [16]byte {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatal(err)
	}
	defer mustClose(t, dev)
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

// holdRootLockAs claims the root directory lock for writing in owner's slot, as if owner held it.
func holdRootLockAs(t *testing.T, imagePath string, owner registeredNode) func() {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(dev)
	if err != nil {
		mustClose(t, dev)
		t.Fatal(err)
	}
	locks := lock.NewManager(dev, sb.LockBitmapOffset, sb.LockBitmapSize, owner.uuid, owner.slot)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := locks.Acquire(ctx, lock.LockTypeInode, uint64(inode.InodeRoot), lock.LockModeExclusive); err != nil {
		mustClose(t, dev)
		t.Fatalf("hold root directory lock as %x: %v", owner.uuid[:8], err)
	}
	return func() {
		if err := locks.Release(lock.LockTypeInode, uint64(inode.InodeRoot)); err != nil {
			t.Errorf("release root directory lock: %v", err)
		}
		mustClose(t, dev)
	}
}

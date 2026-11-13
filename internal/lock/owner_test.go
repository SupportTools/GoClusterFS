package lock

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRegistry answers an OwnerView. An unset slot is empty. A slot in cached
// answers non-fresh reads differently, as a registry read before a change does.
type fakeRegistry struct {
	mu     sync.Mutex
	slots  map[int]OwnerState
	cached map[int]OwnerState
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{slots: map[int]OwnerState{}, cached: map[int]OwnerState{}}
}

func (f *fakeRegistry) set(slot int, state OwnerState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.slots[slot] = state
}

func (f *fakeRegistry) view(slot int, fresh bool) ([16]byte, OwnerState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if state, ok := f.cached[slot]; ok && !fresh {
		return [16]byte{byte(slot)}, state
	}
	if state, ok := f.slots[slot]; ok {
		return [16]byte{byte(slot)}, state
	}
	return [16]byte{}, OwnerReleased
}

func shortenAcquireTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := acquireTimeout
	t.Cleanup(func() { acquireTimeout = old })
	acquireTimeout = d
}

// acquireInBackground starts an Acquire with no deadline and returns its result.
func acquireInBackground(m *Manager, lt LockType, id uint64) <-chan error {
	result := make(chan error, 1)
	go func() { result <- m.Acquire(context.Background(), lt, id, LockModeExclusive) }()
	return result
}

func requireWaiting(t *testing.T, result <-chan error, d time.Duration) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("Acquire returned %v while the holder's replay had not ended", err)
	case <-time.After(d):
	}
}

func requireResult(t *testing.T, result <-chan error, want error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Fatalf("Acquire = %v, want %v", err, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Acquire still waiting after 5s")
	}
}

func TestFailingHoldersOldClaimBlocksUntilRegistrySlotEmpties(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	holder := newTestNode(dev, 0, 1<<20, 1)
	waiter := newTestNode(dev, 0, 1<<20, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerFailing)
	waiter.SetOwnerView(reg.view)

	requireAcquired(t, holder, LockTypeInode, 2, LockModeExclusive)
	ageSlot(t, holder, LockTypeInode, 2, pastStale)
	requireBlocked(t, waiter, LockTypeInode, 2, LockModeExclusive)

	reg.set(1, OwnerReleased)
	requireAcquired(t, waiter, LockTypeInode, 2, LockModeExclusive)
}

func TestWaitForFailingHolderOutlastsTimeout(t *testing.T) {
	shortenAcquireTimeout(t, 300*time.Millisecond)
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	holder := newTestNode(dev, 0, 1<<20, 1)
	waiter := newTestNode(dev, 0, 1<<20, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerFailing)
	waiter.SetOwnerView(reg.view)

	requireAcquired(t, holder, LockTypeInode, 2, LockModeExclusive)
	result := acquireInBackground(waiter, LockTypeInode, 2)
	requireWaiting(t, result, 3*acquireTimeout)

	reg.set(1, OwnerReleased)
	requireResult(t, result, nil)
}

func TestWaitForLiveHolderStillTimesOut(t *testing.T) {
	shortenAcquireTimeout(t, 300*time.Millisecond)
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	holder := newTestNode(dev, 0, 1<<20, 1)
	waiter := newTestNode(dev, 0, 1<<20, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerAlive)
	waiter.SetOwnerView(reg.view)

	requireAcquired(t, holder, LockTypeInode, 2, LockModeExclusive)
	requireResult(t, acquireInBackground(waiter, LockTypeInode, 2), ErrLockTimeout)
}

func TestWaitEndsWhenHoldersReplayLapses(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	holder := newTestNode(dev, 0, 1<<20, 1)
	waiter := newTestNode(dev, 0, 1<<20, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerFailing)
	waiter.SetOwnerView(reg.view)

	requireAcquired(t, holder, LockTypeInode, 2, LockModeExclusive)
	result := acquireInBackground(waiter, LockTypeInode, 2)
	requireWaiting(t, result, 200*time.Millisecond)

	reg.set(1, OwnerLost)
	requireResult(t, result, ErrLockTimeout)
}

// A registry read from before a node registered can show its slot empty; only
// a read from the device after the row may free the claim.
func TestClaimFreesOnlyWhenFreshRegistryReadIsEmpty(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	holder := newTestNode(dev, 0, 1<<20, 1)
	waiter := newTestNode(dev, 0, 1<<20, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerAlive)
	reg.cached[1] = OwnerReleased
	waiter.SetOwnerView(reg.view)

	requireAcquired(t, holder, LockTypeInode, 2, LockModeExclusive)
	requireBlocked(t, waiter, LockTypeInode, 2, LockModeExclusive)
}

func TestTornSlotFreesWhenItsRegistrySlotEmpties(t *testing.T) {
	dev, cleanup := createTestDevice(t, 1<<20)
	defer cleanup()
	waiter := newTestNode(dev, 0, 1<<20, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerFailing)
	waiter.SetOwnerView(reg.view)

	tearSlot(t, waiter, LockTypeInode, 2, 1)
	requireBlocked(t, waiter, LockTypeInode, 2, LockModeExclusive)

	reg.set(1, OwnerReleased)
	requireAcquired(t, waiter, LockTypeInode, 2, LockModeExclusive)
}

// A node that took a dead node's registry slot keeps its predecessor's claims
// while it replays that journal at mount. Peers wait for the replay, however
// long, though the node itself is alive.
func TestPeersWaitOutLiveNodesReplayOfItsPredecessor(t *testing.T) {
	shortenAcquireTimeout(t, 300*time.Millisecond)
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()
	predecessor := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	successor := NewManager(dev, 0, reclaimRegion, [16]byte{5}, 1)
	waiter := NewManager(dev, 0, reclaimRegion, [16]byte{2}, 2)
	reg := newFakeRegistry()
	reg.set(1, OwnerAlive)
	waiter.SetOwnerView(reg.view)

	requireAcquired(t, predecessor, LockTypeInode, 2, LockModeExclusive)
	adopted, err := successor.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v; want the predecessor's record", adopted, err)
	}
	result := acquireInBackground(waiter, LockTypeInode, 2)
	requireWaiting(t, result, 3*acquireTimeout)

	if err := successor.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	requireResult(t, result, nil)
}

// Two nodes each replaying a dead node whose claims share a record: the replay
// of the lower dead slot goes first instead of both waiting on each other.
func TestReplaysSharingRecordTakeTurns(t *testing.T) {
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()
	first := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	second := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	reg := newFakeRegistry()
	reg.set(1, OwnerFailing)
	reg.set(2, OwnerFailing)
	reg.set(3, OwnerAlive)
	reg.set(4, OwnerAlive)
	first.SetOwnerView(reg.view)
	second.SetOwnerView(reg.view)

	writeClaim(t, first, LockTypeInode, 2, 1, LockModeExclusive)
	writeClaim(t, first, LockTypeInode, 2, 2, LockModeExclusive)
	adoptedFirst, err := first.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adoptedFirst) != 1 {
		t.Fatalf("first AdoptOwner = %v, %v", adoptedFirst, err)
	}
	adoptedSecond, err := second.AdoptOwner([16]byte{2}, 2)
	if err != nil || len(adoptedSecond) != 1 {
		t.Fatalf("second AdoptOwner = %v, %v", adoptedSecond, err)
	}

	requireBlocked(t, second, LockTypeInode, 2, LockModeExclusive)
	requireAcquired(t, first, LockTypeInode, 2, LockModeExclusive)
	if err := first.Release(LockTypeInode, 2); err != nil {
		t.Fatal(err)
	}
	first.ReleaseCached()
	if err := first.ReleaseAdopted(adoptedFirst); err != nil {
		t.Fatal(err)
	}
	reg.set(1, OwnerReleased)
	requireAcquired(t, second, LockTypeInode, 2, LockModeExclusive)
}

// The dead node held extent row X, and its journal also changes inode Y. A peer
// holds Y and asks for X, a legal order, while the replay asks for Y. The peer
// gives up after the timeout and lets Y go, and the replay, which waits without
// a limit, takes Y and can finish.
func TestNestedWaitOnReplayingNodeYieldsToReplay(t *testing.T) {
	shortenAcquireTimeout(t, 300*time.Millisecond)
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()
	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	replayer := NewManager(dev, 0, reclaimRegion, [16]byte{3}, 3)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	reg := newFakeRegistry()
	reg.set(1, OwnerFailing)
	reg.set(3, OwnerAlive)
	reg.set(4, OwnerAlive)
	replayer.SetOwnerView(reg.view)
	peer.SetOwnerView(reg.view)
	const inodeY = 2
	extentX := uint64(1)
	for peer.hashToRecord(LockTypeExtent, extentX) == peer.hashToRecord(LockTypeInode, inodeY) {
		extentX++
	}

	requireAcquired(t, dead, LockTypeExtent, extentX, LockModeExclusive)
	adopted, err := replayer.AdoptOwner([16]byte{1}, 1)
	if err != nil || len(adopted) != 1 {
		t.Fatalf("AdoptOwner = %v, %v", adopted, err)
	}

	var logs bytes.Buffer
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	holdsY, askX := make(chan struct{}), make(chan struct{})
	peerResult := make(chan error, 1)
	go func() {
		if err := peer.Acquire(context.Background(), LockTypeInode, inodeY, LockModeExclusive); err != nil {
			peerResult <- err
			return
		}
		close(holdsY)
		<-askX
		err := peer.Acquire(context.Background(), LockTypeExtent, extentX, LockModeExclusive)
		peer.Release(LockTypeInode, inodeY)
		peer.ReleaseCached()
		peerResult <- err
	}()
	<-holdsY
	replayStart := time.Now()
	replayResult := make(chan error, 1)
	go func() {
		replayResult <- replayer.Acquire(ForReplay(context.Background()), LockTypeInode, inodeY, LockModeExclusive)
	}()
	time.Sleep(acquireTimeout / 2)
	close(askX)

	requireResult(t, peerResult, ErrLockTimeout)
	requireResult(t, replayResult, nil)
	if waited := time.Since(replayStart); waited <= acquireTimeout {
		t.Errorf("replay waited %v, want longer than the %v timeout", waited, acquireTimeout)
	}
	log.SetOutput(os.Stderr)
	if want := fmt.Sprintf("held by node %x (slot 1)", [8]byte{1}); !strings.Contains(logs.String(), want) {
		t.Errorf("the peer's timeout does not name the dead node:\n%s", logs.String())
	}

	if err := replayer.Release(LockTypeInode, inodeY); err != nil {
		t.Fatal(err)
	}
	if err := replayer.ReleaseAdopted(adopted); err != nil {
		t.Fatal(err)
	}
	reg.set(1, OwnerReleased)
	requireAcquired(t, peer, LockTypeExtent, extentX, LockModeExclusive)
}

// A caller that can release its locks and retry gets ErrReplayBlocked at once,
// instead of holding them for the timeout while a replay may need one.
func TestRetryableNestedWaitOnFailingHolderReturnsAtOnce(t *testing.T) {
	shortenAcquireTimeout(t, 5*time.Second)
	dev, cleanup := createTestDevice(t, reclaimRegion)
	defer cleanup()
	dead := NewManager(dev, 0, reclaimRegion, [16]byte{1}, 1)
	peer := NewManager(dev, 0, reclaimRegion, [16]byte{4}, 4)
	reg := newFakeRegistry()
	reg.set(1, OwnerFailing)
	reg.set(4, OwnerAlive)
	peer.SetOwnerView(reg.view)
	const inodeY = 2
	extentX := uint64(1)
	for peer.hashToRecord(LockTypeExtent, extentX) == peer.hashToRecord(LockTypeInode, inodeY) {
		extentX++
	}
	requireAcquired(t, dead, LockTypeExtent, extentX, LockModeExclusive)
	requireAcquired(t, peer, LockTypeInode, inodeY, LockModeExclusive)

	start := time.Now()
	err := peer.Acquire(Retryable(context.Background()), LockTypeExtent, extentX, LockModeExclusive)
	if !errors.Is(err, ErrReplayBlocked) || !errors.Is(err, ErrLockTimeout) {
		t.Fatalf("Acquire = %v, want ErrReplayBlocked, which is an ErrLockTimeout", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Errorf("gave up after %v, want at once", took)
	}
}

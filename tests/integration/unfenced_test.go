package integration

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// A file image has no reservations, so its volume has no storage fencing. A
// second node refuses to mount next to a mounted one unless it passes
// -unsafe-allow-unfenced, and with the flag it mounts and warns.
func TestUnfencedVolumeRefusesASecondNode(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 64)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatal(err)
	}

	second := cluster.Node(1)
	out, err := runToExit(t, second.BinaryPath, withoutOverride(cluster, second)...)
	if err == nil || !strings.Contains(out, "has no storage fencing, and the node in slot 0") || !strings.Contains(out, "-unsafe-allow-unfenced") {
		t.Fatalf("a second node without -unsafe-allow-unfenced: err %v; want a refusal naming slot 0 and the flag. Output:\n%s", err, out)
	}

	logs := &syncBuffer{}
	second.Log = logs
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("a second node with -unsafe-allow-unfenced did not mount: %v", err)
	}
	if err := cluster.WaitAllMounted(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "[WARN] UNSAFE") {
		t.Errorf("the second node joined an unfenced volume without the UNSAFE warning. Output:\n%s", logs.String())
	}
}

// withoutOverride is node's mount command line without -unsafe-allow-unfenced.
func withoutOverride(cluster *MultiNodeCluster, node *MountedFS) []string {
	args := []string{"-device", cluster.ImagePath, "-mount", node.MountPoint}
	return append(args, slices.DeleteFunc(slices.Clone(node.portArgs), func(a string) bool { return a == "-unsafe-allow-unfenced" })...)
}

// A crashed node that mounts again on its host takes back its slot with the
// crashed journal. When that mount is refused, the slot must stay for a peer to
// replay, as after a crash; an empty slot's journal is never replayed.
func TestRefusedRemountLeavesTheCrashedJournalToAPeer(t *testing.T) {
	// Big enough that the crashed node reserves blocks ahead, which the replay frees under the bitmap lock.
	cluster := NewMultiNodeCluster(t, 2, 1024)
	peer, node := cluster.Node(0), cluster.Node(1)
	peerLog, nodeLog := &syncBuffer{}, &syncBuffer{}
	peer.Log, node.Log = peerLog, nodeLog
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatal(err)
	}
	offset := mustMatch(t, journalInitRe, nodeLog, "journal init")
	var names []string
	for i := range 5 {
		name := fmt.Sprintf("before-crash-%d", i)
		if err := node.WriteFile(name, []byte(name)); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := cluster.KillNode(1); err != nil {
		t.Fatalf("kill: %v", err)
	}

	out, err := runToExit(t, node.BinaryPath, withoutOverride(cluster, node)...)
	if err == nil || !strings.Contains(out, "has no storage fencing, and the node in slot 0") {
		t.Fatalf("the crashed node's remount without -unsafe-allow-unfenced: err %v; want a refusal. Output:\n%s", err, out)
	}

	replayed := regexp.MustCompile(`\[Recovery\] Journal region: offset=` + offset + `\b`)
	deadline := time.Now().Add(45 * time.Second)
	for !replayed.MatchString(peerLog.String()) {
		if time.Now().After(deadline) {
			t.Fatalf("the peer did not replay the crashed node's journal at offset %s within 45s of the refused remount", offset)
		}
		time.Sleep(500 * time.Millisecond)
	}
	verifyFiles(t, peer, names)
}

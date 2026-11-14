package integration

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"
)

// staleBound is how late a node may see another node's change with no
// broadcasts: the inode cache TTL plus the kernel attribute timeout, with slack.
const staleBound = 5*time.Second + time.Second + 3*time.Second

// Nodes with different broadcast secrets drop each other's messages, as if a
// network partition cut them off while they still share the disk. Each node
// must still see the other's changes within staleBound, and concurrent writes
// to one file must keep both nodes' data.
func TestPartitionedNodesFallBackToCacheTTL(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 400)
	cluster.Node(0).ExtraArgs = []string{"-broadcast-secret=partition-side-a"}
	cluster.Node(1).ExtraArgs = []string{"-broadcast-secret=partition-side-b"}
	logs := [2]*syncBuffer{{}, {}}
	cluster.Node(0).Log, cluster.Node(1).Log = logs[0], logs[1]
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitAllMounted(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	a, b := cluster.Node(0), cluster.Node(1)

	seen := func(what string, cond func() bool) {
		t.Helper()
		took, ok := waitFor(2*staleBound, cond)
		t.Logf("%s showed on the other node after %v", what, took.Round(10*time.Millisecond))
		if !ok || took > staleBound {
			t.Errorf("%s took %v to show on the other node, want at most %v", what, took, staleBound)
		}
	}
	random := func(n int) []byte {
		b := make([]byte, n)
		_, _ = rand.Read(b)
		return b
	}

	first, second := random(8192), random(8192)
	if err := a.WriteFile("f", first); err != nil {
		t.Fatal(err)
	}
	seen("a new file", func() bool { got, err := b.ReadFile("f"); return err == nil && bytes.Equal(got, first) })
	if err := a.WriteFile("f", second); err != nil {
		t.Fatal(err)
	}
	seen("an overwrite", func() bool { got, err := b.ReadFile("f"); return err == nil && bytes.Equal(got, second) })
	grown := append(append([]byte(nil), second...), random(1<<20)...)
	if err := a.WriteFile("f", grown); err != nil {
		t.Fatal(err)
	}
	seen("growth past a block", func() bool { got, err := b.ReadFile("f"); return err == nil && bytes.Equal(got, grown) })
	if err := b.Mkdir("d"); err != nil {
		t.Fatal(err)
	}
	if err := b.WriteFile("d/x", first); err != nil {
		t.Fatal(err)
	}
	seen("a file in a new directory", func() bool {
		entries, err := a.ReadDir("d")
		return err == nil && len(entries) == 1 && entries[0].Name() == "x"
	})
	if err := a.Remove("f"); err != nil {
		t.Fatal(err)
	}
	seen("a delete", func() bool { return !b.Exists("f") })

	const chunk, chunks = 16 << 10, 64
	half := int64(chunk * chunks)
	if err := a.WriteFile("shared", make([]byte, 2*half)); err != nil {
		t.Fatal(err)
	}
	seen("the shared file", func() bool { fi, err := b.Stat("shared"); return err == nil && fi.Size() == 2*half })
	data := [2][]byte{random(int(half)), random(int(half))}
	errs := cluster.RunConcurrent([]int{0, 1}, func(idx int, node *MountedFS) error {
		for i := range chunks {
			off := int64(idx)*half + int64(i*chunk)
			if err := node.WriteFileAt("shared", data[idx][i*chunk:(i+1)*chunk], off); err != nil {
				return fmt.Errorf("node %d chunk %d: %w", idx, i, err)
			}
		}
		return nil
	})
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	want := append(append([]byte(nil), data[0]...), data[1]...)
	for idx, node := range []*MountedFS{a, b} {
		seen(fmt.Sprintf("both halves of the shared file on node %d", idx), func() bool {
			got, err := node.ReadFile("shared")
			return err == nil && bytes.Equal(got, want)
		})
	}

	cluster.StopAllNodes()
	for idx, log := range logs {
		if stats := regexp.MustCompile(`Broadcast stats: sent=\d+, received=\d+`).FindString(log.String()); !strings.HasSuffix(stats, "received=0") {
			t.Errorf("node %d: %q, want no broadcast received across the partition", idx, stats)
		}
	}
	code, out := runFsck(t, cluster.ImagePath)
	if lines := problemLines(out); code != 0 || len(lines) > 0 {
		t.Errorf("fsck after the partitioned run: exit %d:\n%s", code, strings.Join(lines, "\n"))
	}
}

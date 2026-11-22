//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// A crash between a link's increment and its name leaves an extra link, a leak repair clears. Too few links or a
// name held twice can lose data, so the soak fails on them.
func TestIsLeakSortsLinkCountProblems(t *testing.T) {
	for line, want := range map[string]bool{
		"inode 12: regular file has 3 links but 2 names: it will never be freed":                     true,
		"inode 12: regular file has 1 links but 2 names: unlinking can free it while a name remains": false,
		`inode 2: holds the name "l0-f1" more than once; a lookup finds only one of them`:            false,
	} {
		if got := isLeak(line); got != want {
			t.Errorf("isLeak(%q) = %v, want %v", line, got, want)
		}
	}
}

// A read that overlaps a write through another hard link may show the file before or after it. Once the write is in
// the model, only the new data passes.
func TestReadDuringWriteThroughALinkShowsEitherSide(t *testing.T) {
	d := &driver{model: newModel(), running: map[*Op]*File{}}
	for _, op := range []*Op{
		{Kind: "create", Path: "/l1-c1"},
		{Kind: "write", Path: "/l1-c1", Len: 5000, Seed: 1},
		{Kind: "link", Path: "/l1-c1", Path2: "/soak/d3/l1-hl2"},
	} {
		d.model.apply(op)
	}
	write := &Op{Kind: "write", Path: "/soak/d3/l1-hl2", Off: 4000, Len: 3000, Seed: 2}
	before := bytes.Clone(d.model.Files["/l1-c1"].Data)
	after := bytes.Clone(before)
	applyData(write, &after)
	shown := func(b []byte) string { return content(int64(len(b)), sum(b)) }

	done := d.startWrite(write)
	if got, want := d.readable("/l1-c1"), []string{shown(before), shown(after)}; !slices.Equal(got, want) {
		t.Errorf("while the write runs, readable = %v, want %v", got, want)
	}
	d.model.apply(write)
	done()
	if got, want := d.readable("/l1-c1"), []string{shown(after)}; !slices.Equal(got, want) {
		t.Errorf("after the write, readable = %v, want %v", got, want)
	}
}

// A slow check logs its last stale answer, so a review can tell a wrong answer from a slow one.
func TestSlowCheckLogsItsLastStaleAnswer(t *testing.T) {
	var out bytes.Buffer
	d := &driver{cfg: config{window: time.Minute}, logw: &out, ctx: context.Background()}
	answers := []error{errors.New("first"), errors.New("/f: read errno 0 size 1 sum a, want size 2 sum b"), nil}
	if err := d.converge(&node{id: 1}, "node 1 /f", func() error {
		a := answers[0]
		answers = answers[1:]
		return a
	}); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"2 stale answers", "last stale answer: /f: read errno 0 size 1 sum a, want size 2 sum b"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("slow record %q lacks %q", out.String(), want)
		}
	}
}

// A write a fault cut short that resolves as never done must not pass as done in the fault's verify that follows.
func TestCutShortWriteResolvedBeforeAcceptsOnlyTheOldData(t *testing.T) {
	root := t.TempDir()
	d := &driver{cfg: config{window: time.Minute, opTimeout: time.Minute}, model: newModel(), running: map[*Op]*File{}, logw: io.Discard, ctx: context.Background()}
	d.model.apply(&Op{Kind: "create", Path: "/l1-f1"})
	if err := os.WriteFile(filepath.Join(root, "l1-f1"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	n := &node{id: 1, agent: localAgent(t, root)}
	write := &Op{Kind: "write", Path: "/l1-f1", Len: 5000, Seed: 1}
	d.startWrite(write)
	outcome, err := d.resolve(n, write)
	if err != nil || outcome != "before" {
		t.Fatalf("resolve = %q, %v; want before", outcome, err)
	}
	if got, want := d.readable("/l1-f1"), []string{content(0, sum(nil))}; !slices.Equal(got, want) {
		t.Errorf("readable = %v, want only the empty file %v", got, want)
	}
}

// localAgent serves agent requests on root in this process.
func localAgent(t *testing.T, root string) *agentConn {
	reqs, reqw := io.Pipe()
	resps, respw := io.Pipe()
	go func() { _ = respw.CloseWithError(runAgent(root, reqs, respw)) }()
	a := &agentConn{enc: json.NewEncoder(reqw), pending: map[uint64]chan response{}, done: make(chan struct{})}
	go a.read(resps)
	t.Cleanup(func() { _ = reqw.Close() })
	return a
}

func TestFailureReleasesAnOperationAFaultCutShort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &driver{logw: io.Discard, ctx: ctx, cancel: cancel}
	u := &uncertain{op: &Op{}, done: make(chan struct{})}
	released := make(chan struct{})
	go func() {
		d.awaitResolved(u)
		close(released)
	}()
	d.fail("fault power 2: restore: controller: err restore power 2 failed")
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("an operation a failed fault cut short still waits after the run failed, so the soak never ends")
	}
}

// A check of a node a fault hit counts as slow but is marked, and a down node's answers do not count as stale.
func TestSlowCheckDuringAFaultIsMarkedAndNotCountedStale(t *testing.T) {
	var out bytes.Buffer
	d := &driver{cfg: config{window: time.Minute}, logw: &out, ctx: context.Background()}
	n := &node{id: 3}
	n.hit.Store(true)
	answers := []error{errors.New("/f: read errno 2 size 0 sum , want size 1 sum a"), nil}
	if err := d.converge(n, "node 3 /f", func() error {
		a := answers[0]
		answers = answers[1:]
		return a
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"during_fault":true`) || !strings.Contains(out.String(), "during a fault on node 3: node 3 /f took") {
		t.Errorf("slow record %q is not marked as during a fault", out.String())
	}
	if d.slow.Load() != 1 || d.slowDuringFault.Load() != 1 || d.slowStale.Load() != 0 {
		t.Errorf("slow %d, during faults %d, with stale answers %d; want 1, 1, 0", d.slow.Load(), d.slowDuringFault.Load(), d.slowStale.Load())
	}
}

// Outside a fault, a slow check records what its node waited on while it was slow.
func TestSlowCheckRecordsWhatItsNodeWaitedOn(t *testing.T) {
	var out bytes.Buffer
	d := &driver{cfg: config{window: time.Minute}, logw: &out, ctx: context.Background()}
	snaps := []waits{
		{seconds: map[string]float64{"lock inode": 1, "lock file_io": 0.5, "reserve": 0}, reads: 100, readMs: 200, writes: 10, wMs: 50, device: true},
		{seconds: map[string]float64{"lock inode": 4.25, "lock file_io": 0.52, "reserve": 0}, reads: 140, readMs: 600, writes: 10, wMs: 50, device: true},
	}
	d.waitsOf = func(*node) waits {
		w := snaps[0]
		snaps = snaps[1:]
		return w
	}
	if err := d.converge(&node{id: 2}, "node 2 /f", func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	want := "waited on lock inode 3.2s, device 40 at 10.0ms reads, 0 writes"
	if !strings.Contains(out.String(), want) {
		t.Errorf("slow record %q lacks %q", out.String(), want)
	}
	if d.slowStale.Load() != 0 || d.slowDuringFault.Load() != 0 {
		t.Errorf("a slow check with no stale answers outside a fault counted as stale %d, during a fault %d", d.slowStale.Load(), d.slowDuringFault.Load())
	}
}

func TestParseWaitsReadsMetricsAndDiskstats(t *testing.T) {
	w := parseWaits([]byte(`goclusterfs_lock_wait_seconds_total{type="inode"} 7.637581863
goclusterfs_lock_wait_seconds_total{type="file_io"} 5.022810437
goclusterfs_reserve_peer_wait_seconds_total 0.5
device 1200 3400 560 7800
`))
	if w.seconds["lock inode"] != 7.637581863 || w.seconds["lock file_io"] != 5.022810437 || w.seconds["reserve"] != 0.5 {
		t.Errorf("wait seconds %v", w.seconds)
	}
	if !w.device || w.reads != 1200 || w.readMs != 3400 || w.writes != 560 || w.wMs != 7800 {
		t.Errorf("device totals %+v", w)
	}
}

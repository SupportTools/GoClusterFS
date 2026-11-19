//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
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
	if err := d.converge("node 1 /f", func() error {
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

//go:build linux

package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"
)

// A known failure is a filesystem bug whose fix is on the way. A bug hunt may count it instead of failing, to keep
// looking for other bugs. A release-candidate run counts none.

const logStamp = "2006/01/02 15:04:05"

// stampSlack covers VM clocks that run behind or ahead of the host, and whole-second stamps.
const stampSlack = 3000

var (
	failedCreate = regexp.MustCompile(`\[snapshot\] Failed to create '([^']*)' for inode \d+: snapshot in invalid state`)
	queuedDelete = regexp.MustCompile(`\[snapshot/worker\] Queued delete job \d+ for snapshot (\d+)`)
	recoveryDone = []byte("[snapshot/worker] Cleaning up ")
)

func mountLog(checkout string, id int) string {
	return filepath.Join(checkout, "tests", "vm", "results", fmt.Sprintf("vm%d.mount.log", id))
}

// stampOf returns a mount log line's time in Unix ms. VM clocks are UTC.
func stampOf(line []byte) (int64, bool) {
	if len(line) < len(logStamp) {
		return 0, false
	}
	at, err := time.ParseInLocation(logStamp, string(line[:len(logStamp)]), time.UTC)
	return at.UnixMilli(), err == nil
}

func stampedWithin(line []byte, from, to int64) (int64, bool) {
	at, ok := stampOf(line)
	return at, ok && at >= from && at <= to
}

// abandonedByPeer reports whether, between from and to, the node failed to activate snapshot name, and a peer's mount
// abandoned the same snapshot as one a crash left half made. Both log a queued delete of its ID: the node when it rolls
// back, the peer before its "Cleaning up" line.
func abandonedByPeer(own []byte, peers [][]byte, name string, from, to int64) bool {
	ids := map[string]bool{}
	failedAt := int64(-1)
	for _, line := range bytes.Split(own, []byte("\n")) {
		if m := failedCreate.FindSubmatch(line); m != nil && string(m[1]) == name {
			if at, ok := stampedWithin(line, from-stampSlack, to+stampSlack); ok {
				failedAt = at
			}
		} else if m := queuedDelete.FindSubmatch(line); m != nil && failedAt >= 0 {
			if _, ok := stampedWithin(line, failedAt, failedAt+stampSlack); ok {
				ids[string(m[1])] = true
			}
		}
	}
	for _, peer := range peers {
		abandonedAt := int64(-1)
		for _, line := range bytes.Split(peer, []byte("\n")) {
			if m := queuedDelete.FindSubmatch(line); m != nil && ids[string(m[1])] {
				if at, ok := stampedWithin(line, from-stampSlack, to+stampSlack); ok {
					abandonedAt = at
				}
			} else if abandonedAt >= 0 && bytes.Contains(line, recoveryDone) {
				if _, ok := stampedWithin(line, abandonedAt, abandonedAt+stampSlack); ok {
					return true
				}
			}
		}
	}
	return false
}

// knownAbandonedSnapshot handles a snapshot that got EIO because a peer's mount abandoned it while this node created
// it. The node must then show the snapshot as never made.
func (d *driver) knownAbandonedSnapshot(op *Op) bool {
	if !d.cfg.knownAbandon || op.Kind != "snapshot" || op.Got != int(syscall.EIO) {
		return false
	}
	own, err := os.ReadFile(mountLog(d.cfg.checkout, op.Node))
	if err != nil {
		return false
	}
	var peers [][]byte
	for _, m := range d.nodes {
		if m.id == op.Node {
			continue
		}
		if l, err := os.ReadFile(mountLog(d.cfg.checkout, m.id)); err == nil {
			peers = append(peers, l)
		}
	}
	if !abandonedByPeer(own, peers, op.Name, op.At, op.At+op.Ms) {
		return false
	}
	n := d.nodes[op.Node-1]
	var outcome string
	err = d.onNode(n, func() error {
		var err error
		outcome, err = d.resolve(n, op)
		return err
	})
	if err == nil && outcome != "before" {
		err = fmt.Errorf("the node shows %q", outcome)
	}
	if err != nil {
		d.fail("op %d snapshot %s on node %d: a peer's mount abandoned it, but %v", op.Seq, op.Path, op.Node, err)
		return true
	}
	d.knownAbandon.Add(1)
	log.Printf("known: op %d snapshot %s as %s on node %d got EIO because a peer's mount abandoned it", op.Seq, op.Path, op.Name, op.Node)
	d.logRecord(record{Type: "known", Detail: "abandoned by a peer's mount", Op: Op{Seq: op.Seq, Lane: op.Lane, Node: op.Node, At: time.Now().UnixMilli()}})
	return true
}

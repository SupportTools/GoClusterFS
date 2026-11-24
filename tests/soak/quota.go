//go:build linux

package main

import (
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

const quotaXattr = "trusted.goclusterfs.quota"

// quotaUsage is a project's limits and usage as one node reports them: the table plus that node's unflushed changes.
type quotaUsage struct {
	limitBytes, limitInodes, usedBytes, usedInodes int64
}

func parseQuotaUsage(s string) (quotaUsage, error) {
	var u quotaUsage
	fields := map[string]*int64{"bytes": &u.limitBytes, "inodes": &u.limitInodes, "used_bytes": &u.usedBytes, "used_inodes": &u.usedInodes}
	seen := 0
	for _, f := range strings.Fields(s) {
		k, v, _ := strings.Cut(f, "=")
		if dst := fields[k]; dst != nil {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return u, fmt.Errorf("%q: %w", s, err)
			}
			*dst = n
			seen++
		}
	}
	if seen != len(fields) {
		return u, fmt.Errorf("%q: want bytes, inodes, used_bytes and used_inodes", s)
	}
	return u, nil
}

// quotaSlack is how far a project's usage may pass a limit, and how far below it a refusal may come: one allocation
// in flight per lane, the largest a snapshot of a whole file.
func (d *driver) quotaSlack() (bytes, inodes int64) {
	return int64(d.cfg.lanes) * (maxFile + blockSize), int64(d.cfg.lanes)
}

func (d *driver) quotaUsage(n *node, root string) (quotaUsage, error) {
	resp, err := d.call(n, request{Op: "getxattr", Path: root, Attr: quotaXattr})
	if err != nil {
		return quotaUsage{}, err
	}
	if resp.Errno != 0 {
		return quotaUsage{}, fmt.Errorf("getxattr %s %s: errno %d (%s)", root, quotaXattr, resp.Errno, resp.Err)
	}
	return parseQuotaUsage(resp.Text)
}

// overshoot returns an error when u passes a limit by more than the slack.
func (d *driver) overshoot(root string, u quotaUsage) error {
	sb, si := d.quotaSlack()
	if u.limitBytes != 0 && u.usedBytes > u.limitBytes+sb || u.limitInodes != 0 && u.usedInodes > u.limitInodes+si {
		return fmt.Errorf("project %s uses %d bytes and %d inodes, past its limits of %d and %d by more than %d bytes or %d inodes",
			root, u.usedBytes, u.usedInodes, u.limitBytes, u.limitInodes, sb, si)
	}
	return nil
}

// opProject returns the project op's path lies in, or that its new entry lands in.
func opProject(op *Op) string {
	if op.Kind == "create" || op.Kind == "mkdir" || op.Kind == "symlink" {
		return projectOf(path.Dir(op.Path))
	}
	return projectOf(op.Path)
}

// noteFree records when an operation that may free blocks or inodes finished. A snapshot's data inode has no path,
// so a snapshot delete counts in every project.
func (d *driver) noteFree(op *Op) {
	if op.Got != 0 || !slices.Contains([]string{"unlink", "truncate", "rmdir", "rename", "snapdelete", "orphandelete"}, op.Kind) {
		return
	}
	now := time.Now().UnixMilli()
	for root := range quotaDirs {
		if op.Kind == "snapdelete" || op.Kind == "orphandelete" || projectOf(op.Path) == root || projectOf(op.Path2) == root {
			d.lastFree[root].Store(now)
		}
	}
}

// freedLately reports whether an operation freed something in project root within 2 s before op started: a peer's
// unflushed frees still count against the limit until it flushes, each second.
func (d *driver) freedLately(root string, op *Op) bool {
	return d.lastFree[root].Load() >= op.At-2000
}

// checkRefusal checks, on the node that refused op with EDQUOT, that its project is near a limit, unless something
// was freed there lately, and not past one.
func (d *driver) checkRefusal(n *node, op *Op) error {
	root := opProject(op)
	u, err := d.quotaUsage(n, root)
	if err != nil {
		return err
	}
	sb, si := d.quotaSlack()
	if !d.freedLately(root, op) && !(u.limitBytes != 0 && u.usedBytes >= u.limitBytes-sb || u.limitInodes != 0 && u.usedInodes >= u.limitInodes-si) {
		return fmt.Errorf("EDQUOT while project %s uses %d bytes and %d inodes, far below its limits of %d and %d",
			root, u.usedBytes, u.usedInodes, u.limitBytes, u.limitInodes)
	}
	return d.overshoot(root, u)
}

// checkQuotas checks that no project passes a limit by more than the slack, as node n reports usage.
func (d *driver) checkQuotas(n *node) error {
	for _, root := range slices.Sorted(maps.Keys(quotaDirs)) {
		u, err := d.quotaUsage(n, root)
		if err != nil {
			return err
		}
		if err := d.overshoot(root, u); err != nil {
			return err
		}
	}
	return nil
}

// setQuotas makes each quota directory a project with its limits.
func (d *driver) setQuotas(n *node) error {
	for _, root := range slices.Sorted(maps.Keys(quotaDirs)) {
		resp, err := d.call(n, request{Op: "setxattr", Path: root, Attr: quotaXattr, Value: quotaDirs[root]})
		if err != nil {
			return err
		}
		if resp.Errno != 0 {
			return fmt.Errorf("setxattr %s %s=%q: errno %d (%s)", root, quotaXattr, quotaDirs[root], resp.Errno, resp.Err)
		}
	}
	return nil
}

// isQuotaDrift reports an fsck line about a project whose table usage differs from what its inodes use.
func isQuotaDrift(line string) bool { return strings.Contains(line, "the quota table counts") }

// checkDrift checks an fsck drift line against what faults may leave: usage high, never low, by at most one
// operation in flight per lane on each faulted node, sized as a snapshot of a whole file, and one inode for each.
func (d *driver) checkDrift(line string, faults int64) error {
	var table, tableInodes, use, useInodes int64
	i := strings.Index(line, "the quota table counts")
	if i < 0 {
		return fmt.Errorf("unparsed drift: %s", line)
	}
	if _, err := fmt.Sscanf(line[i:], "the quota table counts %d blocks and %d inodes, but the project's inodes use %d blocks and %d inodes",
		&table, &tableInodes, &use, &useInodes); err != nil {
		return fmt.Errorf("unparsed drift %q: %w", line, err)
	}
	bs := int64(d.cfg.blockSize)
	maxBlocks := faults * int64(d.cfg.lanes) * ((maxFile + blockSize + bs - 1) / bs)
	maxInodes := faults * int64(d.cfg.lanes)
	if up, upInodes := table-use, tableInodes-useInodes; up < 0 || upInodes < 0 || up > maxBlocks || upInodes > maxInodes {
		return fmt.Errorf("usage drifted by %d blocks and %d inodes, outside 0 to %d blocks and 0 to %d inodes for %d faults: %s",
			up, upInodes, maxBlocks, maxInodes, faults, line)
	}
	return nil
}

// checkStatfs checks that df on each project root shows its limits: the byte limit as the size, the inode limit as
// the inode count.
func (d *driver) checkStatfs(n *node) error {
	for _, root := range slices.Sorted(maps.Keys(quotaDirs)) {
		want, err := parseQuotaUsage(quotaDirs[root] + " used_bytes=0 used_inodes=0")
		if err != nil {
			return err
		}
		resp, err := d.call(n, request{Op: "statfs", Path: root})
		if err != nil {
			return err
		}
		var size, files int64
		if _, err := fmt.Sscan(resp.Text, &size, &files); resp.Errno != 0 || err != nil {
			return fmt.Errorf("statfs %s: errno %d (%s) %v", root, resp.Errno, resp.Err, err)
		}
		if want.limitBytes != 0 && size != want.limitBytes || want.limitInodes != 0 && files != want.limitInodes {
			return fmt.Errorf("df on %s shows %d bytes and %d inodes, want its limits %d and %d", root, size, files, want.limitBytes, want.limitInodes)
		}
	}
	return nil
}

// quotaAgreement is how long nodes may take to report the same usage once operations stop: each flushes every second.
const quotaAgreement = 3 * time.Second

// checkAgreement checks that every node reports the same usage of each project within quotaAgreement. The caller
// holds every lane paused.
func (d *driver) checkAgreement(nodes []*node) error {
	if len(nodes) < 2 {
		return nil
	}
	deadline := time.Now().Add(quotaAgreement)
	for {
		seen, err := d.usageByNode(nodes)
		if err != nil || len(seen) == 1 {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("nodes still report different project usage %v after operations stopped: %v", quotaAgreement, seen)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// usageByNode maps each distinct usage report, every project's, to the nodes that gave it.
func (d *driver) usageByNode(nodes []*node) (map[string][]int, error) {
	seen := map[string][]int{}
	for _, n := range nodes {
		var report []string
		for _, root := range slices.Sorted(maps.Keys(quotaDirs)) {
			u, err := d.quotaUsage(n, root)
			if err != nil {
				return nil, fmt.Errorf("node %d: %w", n.id, err)
			}
			report = append(report, fmt.Sprintf("%s=%d/%d", root, u.usedBytes, u.usedInodes))
		}
		key := strings.Join(report, " ")
		seen[key] = append(seen[key], n.id)
	}
	return seen, nil
}

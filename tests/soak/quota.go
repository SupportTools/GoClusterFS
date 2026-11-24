//go:build linux

package main

import (
	"fmt"
	"maps"
	"path"
	"slices"
	"strconv"
	"strings"
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

// checkRefusal checks, on the node that refused op with EDQUOT, that its project is near a limit and not past one.
func (d *driver) checkRefusal(n *node, op *Op) error {
	root := projectOf(op.Path)
	if op.Kind == "create" || op.Kind == "mkdir" || op.Kind == "symlink" {
		root = projectOf(path.Dir(op.Path))
	}
	u, err := d.quotaUsage(n, root)
	if err != nil {
		return err
	}
	sb, si := d.quotaSlack()
	if !(u.limitBytes != 0 && u.usedBytes >= u.limitBytes-sb || u.limitInodes != 0 && u.usedInodes >= u.limitInodes-si) {
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

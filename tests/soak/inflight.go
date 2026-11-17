//go:build linux

package main

import (
	"bytes"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"syscall"
)

// uncertain is an operation a fault cut short. Its lane waits until the fault reads what it left.
type uncertain struct {
	op   *Op
	done chan struct{}
}

// resolvePending decides, for each operation a fault cut short on n, whether it took effect, from what n shows after
// recovery, and updates the model to match. The caller holds n exclusively.
func (d *driver) resolvePending(n *node) error {
	d.pendingMu.Lock()
	cut := d.pending[n.id]
	delete(d.pending, n.id)
	d.pendingMu.Unlock()
	defer func() {
		for _, u := range cut {
			close(u.done)
		}
	}()
	for _, u := range cut {
		outcome, err := d.resolve(n, u.op)
		if err != nil {
			return fmt.Errorf("op %d %s %s cut short: %w", u.op.Seq, u.op.Kind, u.op.Path, err)
		}
		d.resolved.Add(1)
		d.logRecord(record{Type: "resolved", Detail: outcome, Op: Op{Seq: u.op.Seq, Lane: u.op.Lane, Node: u.op.Node}})
	}
	return nil
}

var errUnsettled = errors.New("not settled yet")

// resolve returns "before", "after" or, for a write, "torn", and applies the outcome to the model.
func (d *driver) resolve(n *node, op *Op) (string, error) {
	if op.Want != 0 {
		return "before", nil
	}
	var outcome string
	err := d.converge(fmt.Sprintf("node %d resolving op %d", n.id, op.Seq), func() error {
		var err error
		outcome, err = d.observe(n, op)
		return err
	})
	if err != nil {
		return "", err
	}
	if outcome == "after" {
		d.model.mu.Lock()
		d.model.apply(op)
		d.model.mu.Unlock()
	}
	return outcome, nil
}

// observe reads what node n shows for op's paths and returns which outcome it matches.
func (d *driver) observe(n *node, op *Op) (string, error) {
	d.model.mu.Lock()
	f := d.model.Files[op.Path]
	var before []byte
	if f != nil {
		before = bytes.Clone(f.Data)
	}
	var snapData []byte
	if s := d.model.Snaps[op.Name]; s != nil {
		snapData = s.Data
	}
	_, isLink := d.model.Links[op.Path]
	isDir := d.model.Dirs[op.Path]
	_, dstLink := d.model.Links[op.Path2]
	dstDir, dstExisted := d.model.Dirs[op.Path2], d.model.exists(op.Path2)
	d.model.mu.Unlock()

	switch op.Kind {
	case "create", "mkdir", "unlink", "rmdir", "symlink":
		exists, err := d.present(n, op.Path, op.Kind == "mkdir" || op.Kind == "rmdir", isLink || op.Kind == "symlink")
		if err != nil {
			return "", err
		}
		if exists == (op.Kind == "create" || op.Kind == "mkdir" || op.Kind == "symlink") {
			return "after", nil
		}
		return "before", nil
	case "rename":
		srcThere, err := d.present(n, op.Path, isDir, isLink)
		if err != nil {
			return "", err
		}
		// Before the rename the target is as it was; after it, the target is the source.
		dstThere, err := d.present(n, op.Path2, dstDir, dstLink)
		if srcThere {
			if err != nil {
				return "", err
			}
			if dstThere == dstExisted {
				return "before", nil
			}
		} else {
			if dstThere, err = d.present(n, op.Path2, isDir, isLink); err != nil {
				return "", err
			}
			if dstThere && (f == nil || d.sameContent(n, op.Path2, before)) {
				return "after", nil
			}
		}
		return "", fmt.Errorf("rename %s to %s: source shown %v, target shown %v, which is neither before nor after", op.Path, op.Path2, srcThere, dstThere)
	case "write", "truncate":
		after := bytes.Clone(before)
		applyData(op, &after)
		resp, err := d.call(n, request{Op: "readall", Path: op.Path})
		if err != nil {
			return "", err
		}
		if resp.Errno != 0 {
			return "", fmt.Errorf("%s: read errno %d", op.Path, resp.Errno)
		}
		switch {
		case bytes.Equal(resp.Data, after):
			return "after", nil
		case bytes.Equal(resp.Data, before):
			return "before", nil
		case op.Kind == "write" && tornBetween(resp.Data, before, after):
			d.model.mu.Lock()
			d.model.Files[op.Path].Data = resp.Data
			d.model.mu.Unlock()
			return "torn", nil
		}
		return "", fmt.Errorf("%s: %d bytes that are neither before (%d) nor after (%d) the operation", op.Path, len(resp.Data), len(before), len(after))
	case "snapshot", "snapdelete":
		state, err := d.snapState(n, op.Path, op.Name)
		if err != nil {
			return "", err
		}
		switch {
		case state == "creating" || state == "deleting":
			return "", fmt.Errorf("snapshot %s is %s: %w", op.Name, state, errUnsettled)
		case (state == "active") == (op.Kind == "snapshot"):
			return "after", nil
		}
		return "before", nil
	case "clone", "snapclone":
		source := before
		if op.Kind == "snapclone" {
			source = snapData
		}
		resp, err := d.call(n, request{Op: "read", Path: op.Path2})
		if err != nil {
			return "", err
		}
		switch {
		case resp.Errno == int(syscall.ENOENT):
			return "before", nil
		case resp.Errno == 0 && resp.Size == int64(len(source)) && resp.Sum == sum(source):
			return "after", nil
		}
		return "", fmt.Errorf("%s: errno %d size %d, neither absent nor a full copy", op.Path2, resp.Errno, resp.Size)
	}
	return "", fmt.Errorf("cannot resolve a cut-short %s", op.Kind)
}

// sameContent reports whether node n reads want at p.
func (d *driver) sameContent(n *node, p string, want []byte) bool {
	resp, err := d.call(n, request{Op: "read", Path: p})
	return err == nil && resp.Errno == 0 && resp.Size == int64(len(want)) && resp.Sum == sum(want)
}

// present reports whether node n shows p. A link is looked up in its directory's listing, since reading follows it.
func (d *driver) present(n *node, p string, dir, link bool) (bool, error) {
	if !link {
		return d.exists(n, p, dir)
	}
	resp, err := d.call(n, request{Op: "list", Path: path.Dir(p)})
	if err != nil {
		return false, err
	}
	if resp.Errno != 0 {
		return false, fmt.Errorf("%s: list errno %d", path.Dir(p), resp.Errno)
	}
	return slices.ContainsFunc(resp.Names, func(e string) bool { return strings.HasPrefix(e, "l:"+path.Base(p)+":") }), nil
}

func (d *driver) exists(n *node, p string, dir bool) (bool, error) {
	req := request{Op: "read", Path: p}
	if dir {
		req.Op = "list"
	}
	resp, err := d.call(n, req)
	if err != nil {
		return false, err
	}
	switch resp.Errno {
	case 0:
		return true, nil
	case int(syscall.ENOENT):
		return false, nil
	}
	return false, fmt.Errorf("%s: errno %d", p, resp.Errno)
}

// snapState returns the state node n lists for snapshot name of the file at p, or "" if it lists none.
func (d *driver) snapState(n *node, p, name string) (string, error) {
	resp, err := d.call(n, request{Op: "getxattr", Path: p, Attr: "user.snapshots"})
	if err != nil {
		return "", err
	}
	if resp.Errno == int(syscall.ENOENT) {
		return "", nil
	}
	if resp.Errno != 0 {
		return "", fmt.Errorf("%s: snapshots errno %d", p, resp.Errno)
	}
	for _, line := range strings.FieldsFunc(resp.Text, func(r rune) bool { return r == '\n' || r == ',' }) {
		if got, state, ok := strings.Cut(line, ":"); ok && got == name {
			return state, nil
		}
	}
	return "", nil
}

// applyData changes data as a successful write or truncate would.
func applyData(op *Op, data *[]byte) {
	switch op.Kind {
	case "write":
		if end := op.Off + op.Len; end > int64(len(*data)) {
			*data = append(*data, make([]byte, end-int64(len(*data)))...)
		}
		copy((*data)[op.Off:], genData(op.Seed, op.Len))
	case "truncate":
		if op.Len < int64(len(*data)) {
			*data = (*data)[:op.Len]
		} else {
			*data = append(*data, make([]byte, op.Len-int64(len(*data)))...)
		}
	}
}

// tornBetween reports whether got could be a write cut short: no longer than after, and each 4 KB sector as before
// or as after, with bytes past either one's end reading as zeros.
func tornBetween(got, before, after []byte) bool {
	if len(got) > len(after) || len(got) < len(before) {
		return false
	}
	const sector = 4096
	for off := 0; off < len(got); off += sector {
		end := min(off+sector, len(got))
		if !slices.Equal(got[off:end], padded(before, off, end)) && !slices.Equal(got[off:end], padded(after, off, end)) {
			return false
		}
	}
	return true
}

func padded(b []byte, from, to int) []byte {
	out := make([]byte, to-from)
	if from < len(b) {
		copy(out, b[from:min(to, len(b))])
	}
	return out
}

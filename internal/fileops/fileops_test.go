package fileops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/snapshot"
)

type fakeFS struct {
	snapshots bool
	calls     []string
}

func (f *fakeFS) SnapshotsEnabled() bool { return f.snapshots }
func (f *fakeFS) AllSnapshots(context.Context) ([]snapshot.Entry, error) {
	f.calls = append(f.calls, "all")
	return nil, nil
}
func (f *fakeFS) SnapshotsAt(_ context.Context, p string) ([]*snapshot.Descriptor, error) {
	f.calls = append(f.calls, "at "+p)
	return nil, nil
}
func (f *fakeFS) DeleteOrphanSnapshot(_ context.Context, name string) error {
	f.calls = append(f.calls, "orphan "+name)
	return nil
}
func (f *fakeFS) SnapshotFile(_ context.Context, p, name string) error {
	f.calls = append(f.calls, "snapshot "+p+" "+name)
	return nil
}
func (f *fakeFS) DeleteFileSnapshot(_ context.Context, p, name string) error {
	f.calls = append(f.calls, "delete "+p+" "+name)
	return nil
}
func (f *fakeFS) CloneFileSnapshot(_ context.Context, p, name, dest string) error {
	f.calls = append(f.calls, "clonesnap "+p+" "+name+" "+dest)
	return nil
}
func (f *fakeFS) CloneFile(_ context.Context, p, dest string) error {
	f.calls = append(f.calls, "clone "+p+" "+dest)
	return nil
}

func (f *fakeFS) ProjectQuota(_ context.Context, p string) (*quota.Status, error) {
	f.calls = append(f.calls, "quota "+p)
	return &quota.Status{Project: 7, LimitBytes: 1 << 20, LimitInodes: 10, UsedBytes: 4096, UsedInodes: 2}, nil
}
func (f *fakeFS) SetProjectQuota(_ context.Context, p string, bytes, inodes uint64) (*quota.Status, error) {
	f.calls = append(f.calls, fmt.Sprintf("setquota %s %d %d", p, bytes, inodes))
	return &quota.Status{Project: 7, LimitBytes: bytes, LimitInodes: inodes}, nil
}

// The quota requests reach the FS only with an absolute, clean path, the root included, and return its figures.
func TestQuotaOps(t *testing.T) {
	ctx := context.Background()
	fsys := &fakeFS{}
	o := New(fsys)
	got, err := o.Quota(ctx, "/pvc")
	if err != nil {
		t.Fatal(err)
	}
	if want := (api.QuotaInfo{Path: "/pvc", Project: 7, Bytes: 1 << 20, Inodes: 10, UsedBytes: 4096, UsedInodes: 2}); *got != want {
		t.Errorf("Quota = %+v, want %+v", *got, want)
	}
	if got, err = o.SetQuota(ctx, "/pvc", 5<<20, 3); err != nil || got.Bytes != 5<<20 || got.Inodes != 3 {
		t.Errorf("SetQuota = %+v, %v; want the new limits", got, err)
	}
	if _, err := o.Quota(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"", "pvc", "/pvc/", "/a/../b", "/a\x00b"} {
		if _, err := o.Quota(ctx, p); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("Quota(%q) = %v, want ErrInvalidPath", p, err)
		}
		if _, err := o.SetQuota(ctx, p, 1, 1); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("SetQuota(%q) = %v, want ErrInvalidPath", p, err)
		}
	}
	if got, want := strings.Join(fsys.calls, "|"), "quota /pvc|setquota /pvc 5242880 3|quota /"; got != want {
		t.Errorf("FS calls %q, want %q", got, want)
	}
}

func TestPathChecks(t *testing.T) {
	for _, p := range []string{"", "f", "/", "/a/../b", "/a/", "//a", "/a/./b", "/a\x00b"} {
		if err := checkPath(p); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("checkPath(%q) = %v, want ErrInvalidPath", p, err)
		}
	}
	for _, p := range []string{"/f", "/dir/f", "/a b/c"} {
		if err := checkPath(p); err != nil {
			t.Errorf("checkPath(%q) = %v", p, err)
		}
	}
	for _, n := range []string{"", "a:b", "a/b", "a\x00"} {
		if err := checkName(n); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("checkName(%q) = %v, want ErrInvalidPath", n, err)
		}
	}
	for _, d := range []string{"", "c", "/", "/dir/c", "/../c", "/c/", "/a:b"} {
		if err := checkDest(d); !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("checkDest(%q) = %v, want ErrInvalidPath", d, err)
		}
	}
	if err := checkDest("/copy"); err != nil {
		t.Errorf("checkDest(/copy) = %v", err)
	}
}

func TestOpsCallTheFSOnlyWithCheckedArguments(t *testing.T) {
	ctx := context.Background()
	fsys := &fakeFS{snapshots: true}
	o := New(fsys)
	for _, err := range []error{
		o.CreateSnapshot(ctx, "/d/f", "s"),
		o.DeleteSnapshot(ctx, "/d/f", "a/b:c"),
		o.DeleteSnapshot(ctx, "", "orphan"),
		o.CloneSnapshot(ctx, "/d/f", "s", "/c"),
		o.CloneFile(ctx, "/d/f", "/c"),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o.ListSnapshots(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ListSnapshots(ctx, "/d/f"); err != nil {
		t.Fatal(err)
	}
	want := "snapshot /d/f s|delete /d/f a/b:c|orphan orphan|clonesnap /d/f s /c|clone /d/f /c|all|at /d/f"
	if got := strings.Join(fsys.calls, "|"); got != want {
		t.Fatalf("FS calls:\n%s\nwant\n%s", got, want)
	}

	fsys.calls = nil
	for _, err := range []error{
		o.CreateSnapshot(ctx, "/d/../f", "s"),
		o.CreateSnapshot(ctx, "/f", "a:b"),
		o.DeleteSnapshot(ctx, "f", "s"),
		o.CloneSnapshot(ctx, "/f", "s", "/d/c"),
		o.CloneFile(ctx, "/", "/c"),
	} {
		if !errors.Is(err, api.ErrInvalidPath) {
			t.Errorf("bad request: %v, want ErrInvalidPath", err)
		}
	}
	if _, err := o.ListSnapshots(ctx, "/d/"); !errors.Is(err, api.ErrInvalidPath) {
		t.Errorf("list with a bad path: %v, want ErrInvalidPath", err)
	}
	if len(fsys.calls) != 0 {
		t.Errorf("refused requests reached the FS: %v", fsys.calls)
	}
}

func TestSnapshotsOff(t *testing.T) {
	ctx := context.Background()
	o := New(&fakeFS{})
	if _, err := o.ListSnapshots(ctx, ""); !errors.Is(err, api.ErrDisabled) {
		t.Errorf("list: %v, want ErrDisabled", err)
	}
	if err := o.DeleteSnapshot(ctx, "", "s"); !errors.Is(err, api.ErrDisabled) {
		t.Errorf("orphan delete: %v, want ErrDisabled", err)
	}
}

package integration

import "testing"

// Git, SQLite and Postgres fsync a directory after they create or remove an entry in it. Each such fsync succeeds.
func TestDirectoryFsync(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	fsyncDir := func(name string) {
		t.Helper()
		if err := syncPath(mfs.Path(name)); err != nil {
			t.Errorf("fsync of directory %q: %v", name, err)
		}
	}

	if err := mfs.WriteFile("created.txt", []byte("x")); err != nil {
		t.Fatal(err)
	}
	fsyncDir(".")
	if err := mfs.Mkdir("sub"); err != nil {
		t.Fatal(err)
	}
	fsyncDir(".")
	if err := mfs.WriteFile("sub/inner.txt", []byte("y")); err != nil {
		t.Fatal(err)
	}
	fsyncDir("sub")
	if err := mfs.Remove("created.txt"); err != nil {
		t.Fatal(err)
	}
	fsyncDir(".")
}

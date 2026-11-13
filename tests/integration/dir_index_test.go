package integration

import (
	"fmt"
	"os"
	"testing"
)

// With broadcast and the kernel cache off, only the on-disk generation check can show the reader the writer's changes.
func TestMultiNodeDirIndexWithoutBroadcast(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 50)
	cluster.Node(0).ExtraArgs = []string{"-broadcast=false"}
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("Failed to format and start first node: %v", err)
	}
	cluster.Node(1).ExtraArgs = []string{"-broadcast=false", "-attr-timeout=0"}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("Failed to start second node: %v", err)
	}
	writer, reader := cluster.Node(0), cluster.Node(1)

	if err := writer.Mkdir("d"); err != nil {
		t.Fatalf("mkdir d: %v", err)
	}
	for i := range 200 {
		writeOrFail(t, writer, fmt.Sprintf("d/f%d", i), []byte("x"), 0)
	}
	for i := range 200 {
		if _, err := reader.Stat(fmt.Sprintf("d/f%d", i)); err != nil {
			t.Fatalf("reader: stat d/f%d: %v", i, err)
		}
	}
	if _, err := reader.Stat("d/new"); !os.IsNotExist(err) {
		t.Fatalf("reader: stat d/new before it exists: %v", err)
	}

	writeOrFail(t, writer, "d/new", []byte("new"), 0)
	expectContent(t, reader, "d/new", []byte("new"))

	if err := writer.Remove("d/f5"); err != nil {
		t.Fatalf("writer: remove d/f5: %v", err)
	}
	if _, err := reader.Stat("d/f5"); !os.IsNotExist(err) {
		t.Errorf("reader: stat d/f5 after the writer removed it: %v", err)
	}

	if err := writer.Remove("d/f6"); err != nil {
		t.Fatalf("writer: remove d/f6: %v", err)
	}
	writeOrFail(t, writer, "d/f6", []byte("again"), 0)
	expectContent(t, reader, "d/f6", []byte("again"))
}

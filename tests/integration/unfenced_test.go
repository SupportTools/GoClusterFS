package integration

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// A file image has no reservations, so its volume has no storage fencing. A
// second node refuses to mount next to a mounted one unless it passes
// -unsafe-allow-unfenced, and with the flag it mounts and warns.
func TestUnfencedVolumeRefusesASecondNode(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 64)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatal(err)
	}

	second := cluster.Node(1)
	args := []string{"-device", cluster.ImagePath, "-mount", second.MountPoint}
	args = append(args, slices.DeleteFunc(slices.Clone(second.portArgs), func(a string) bool { return a == "-unsafe-allow-unfenced" })...)
	out, err := runToExit(t, second.BinaryPath, args...)
	if err == nil || !strings.Contains(out, "has no storage fencing, and the node in slot 0") || !strings.Contains(out, "-unsafe-allow-unfenced") {
		t.Fatalf("a second node without -unsafe-allow-unfenced: err %v; want a refusal naming slot 0 and the flag. Output:\n%s", err, out)
	}

	logs := &syncBuffer{}
	second.Log = logs
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("a second node with -unsafe-allow-unfenced did not mount: %v", err)
	}
	if err := cluster.WaitAllMounted(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "[WARN] UNSAFE") {
		t.Errorf("the second node joined an unfenced volume without the UNSAFE warning. Output:\n%s", logs.String())
	}
}

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// -enable-quotas refuses while a node's heartbeat changes, since a running node of this build would allocate without
// counting, and enables once that node stopped.
func TestEnableQuotasRefusesWhileANodeRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fs.img")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, superblock.MinDeviceSize); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	sb, err := superblock.Format(dev, 0)
	if err != nil {
		t.Fatal(err)
	}
	quotas := func() bool {
		t.Helper()
		onDisk, err := superblock.Read(dev)
		if err != nil {
			t.Fatal(err)
		}
		return onDisk.RequiredFlags&superblock.RequiredProjectQuotas != 0
	}

	reg := node.NewRegistry(dev, sb.NodeRegistryOffset)
	if _, err := reg.Register(context.Background(), "node-a"); err != nil {
		t.Fatal(err)
	}
	if err := reg.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	if err := enableQuotas(&bytes.Buffer{}, dev, path); err == nil || !strings.Contains(err.Error(), "mounted by") || quotas() {
		t.Fatalf("enableQuotas with a node running: %v, quotas on %v; want a refusal naming the mount and no quotas", err, quotas())
	}

	reg.Stop()
	var out bytes.Buffer
	if err := enableQuotas(&out, dev, path); err != nil || !quotas() {
		t.Fatalf("enableQuotas after the node stopped: %v, quotas on %v; want quotas on. Output:\n%s", err, quotas(), out.String())
	}
}

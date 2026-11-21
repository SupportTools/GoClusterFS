package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// user is a caller other than the mount's: its uid, primary group and supplementary groups.
type user struct {
	uid, gid uint32
	groups   []uint32
}

// runAs runs a shell script as u. It needs root, or sudo without a password.
func runAs(u user, script string) (string, error) {
	var cmd *exec.Cmd
	if os.Geteuid() == 0 {
		cmd = exec.Command("sh", "-c", script)
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: u.uid, Gid: u.gid, Groups: u.groups}}
	} else {
		groups := "--clear-groups"
		if len(u.groups) > 0 {
			ids := make([]string, len(u.groups))
			for i, g := range u.groups {
				ids[i] = fmt.Sprint(g)
			}
			groups = "--groups=" + strings.Join(ids, ",")
		}
		cmd = exec.Command("sudo", "-n", "setpriv", fmt.Sprintf("--reuid=%d", u.uid), fmt.Sprintf("--regid=%d", u.gid), groups, "--", "sh", "-c", script)
	}
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return strings.TrimSpace(out.String()), err
}

// skipWithoutOtherUsers skips unless this test can act as other users and mount with -allow-other.
func skipWithoutOtherUsers(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		return
	}
	if err := exec.Command("sudo", "-n", "setpriv", "--version").Run(); err != nil {
		t.Skip("needs root, or sudo without a password and setpriv, to act as other users")
	}
	conf, _ := os.ReadFile("/etc/fuse.conf")
	for _, line := range strings.Split(string(conf), "\n") {
		if strings.TrimSpace(line) == "user_allow_other" {
			return
		}
	}
	t.Skip("a mount by a user other than root needs user_allow_other in /etc/fuse.conf for -allow-other")
}

// With -allow-other on two nodes, the kernel lets users other than root do what each file's mode bits allow.
func TestNonRootUsersAcrossNodes(t *testing.T) {
	skipWithoutOtherUsers(t)
	if fi, err := os.Stat(os.TempDir()); err != nil || fi.Mode().Perm()&0o001 == 0 {
		t.Skipf("other users cannot search %s, where the mount points go", os.TempDir())
	}
	cluster := NewMultiNodeCluster(t, 2, 128)
	for i := range 2 {
		cluster.Node(i).ExtraArgs = append(cluster.Node(i).ExtraArgs, "-allow-other")
	}
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and mount node 0: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("mount node 1: %v", err)
	}
	if err := cluster.WaitAllMounted(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	n0, n1 := cluster.Node(0).MountPoint, cluster.Node(1).MountPoint
	for _, mnt := range []string{n0, n1} {
		mounts, _ := os.ReadFile("/proc/mounts")
		for _, line := range strings.Split(string(mounts), "\n") {
			if f := strings.Fields(line); len(f) > 3 && f[1] == mnt && !strings.Contains(f[3], "nosuid") {
				t.Errorf("%s is mounted %s; want nosuid, so a setuid bit on the volume grants nothing", mnt, f[3])
			}
		}
	}

	const team = 3000
	root := user{0, 0, nil}
	alice := user{2001, 2001, []uint32{2001, team}}
	bob := user{2002, 2002, []uint32{2002, team}}
	carol := user{2003, 2003, nil}
	must := func(u user, script string) string {
		t.Helper()
		out, err := runAs(u, script)
		if err != nil {
			t.Fatalf("as uid %d: %s: %v: %s", u.uid, script, err, out)
		}
		return out
	}
	refused := func(u user, script, why string) {
		t.Helper()
		out, err := runAs(u, script)
		if err == nil || !strings.Contains(out, why) {
			t.Errorf("as uid %d: %s: got %v %q, want it refused with %q", u.uid, script, err, out, why)
		}
	}
	const denied, notPermitted = "Permission denied", "Operation not permitted"
	at := func(mnt string, path ...string) string { return filepath.Join(append([]string{mnt}, path...)...) }

	must(root, fmt.Sprintf("mkdir %s %s && chown %d:%d %s && chmod 2770 %s && chmod 1777 %s",
		at(n0, "team"), at(n0, "pub"), alice.uid, team, at(n0, "team"), at(n0, "team"), at(n0, "pub")))
	must(alice, fmt.Sprintf("umask 027 && echo secret > %s && mkdir %s", at(n0, "team", "a.txt"), at(n0, "team", "sub")))

	if got, want := must(root, fmt.Sprintf("stat -c '%%u %%g %%a' %s %s", at(n1, "team", "a.txt"), at(n1, "team", "sub"))),
		fmt.Sprintf("%d %d 640\n%d %d 2750", alice.uid, team, alice.uid, team); got != want {
		t.Errorf("owner, group and mode of alice's file and directory on node 1:\n%s\nwant (the setgid directory's group, and setgid on the new directory):\n%s", got, want)
	}

	if got := must(alice, "cat "+at(n1, "team", "a.txt")); got != "secret" {
		t.Errorf("alice reads her file on node 1: %q", got)
	}
	must(alice, "echo more >> "+at(n1, "team", "a.txt"))
	if got := must(bob, "cat "+at(n1, "team", "a.txt")); got != "secret\nmore" {
		t.Errorf("bob, in the file's group, reads it on node 1: %q", got)
	}
	refused(bob, "echo bob >> "+at(n1, "team", "a.txt"), denied)
	must(bob, "touch "+at(n1, "team", "bob.txt"))
	if got := must(root, "stat -c '%u %g' "+at(n0, "team", "bob.txt")); got != fmt.Sprintf("%d %d", bob.uid, team) {
		t.Errorf("bob's new file in the setgid directory: %s, want uid %d gid %d", got, bob.uid, team)
	}
	refused(carol, "ls "+at(n1, "team"), denied)
	refused(carol, "cat "+at(n1, "team", "a.txt"), denied)

	refused(bob, "chmod 666 "+at(n0, "team", "a.txt"), notPermitted)
	must(alice, fmt.Sprintf("chgrp %d %s && chgrp %d %s", alice.gid, at(n0, "team", "a.txt"), team, at(n0, "team", "a.txt")))
	refused(alice, fmt.Sprintf("chown %d %s", bob.uid, at(n0, "team", "a.txt")), notPermitted)
	must(root, fmt.Sprintf("chown %d %s", bob.uid, at(n0, "team", "a.txt")))

	must(alice, "touch "+at(n0, "pub", "alice.txt"))
	refused(bob, "rm -f "+at(n1, "pub", "alice.txt"), notPermitted)
	refused(bob, fmt.Sprintf("mv %s %s", at(n1, "pub", "alice.txt"), at(n1, "pub", "bob.txt")), notPermitted)
	must(alice, "rm "+at(n1, "pub", "alice.txt"))
}

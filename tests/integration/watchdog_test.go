package integration

import "testing"

func TestWatchdogFindsTheMountsOwnFUSEConnection(t *testing.T) {
	const (
		root  = "22 1 259:2 / / rw,relatime shared:1 - ext4 /dev/nvme0n1p2 rw\n"
		tmp   = "30 22 0:31 / /tmp rw,nosuid,nodev shared:5 - tmpfs tmpfs rw\n"
		mnt   = "41 30 0:48 / /tmp/gcfs_mnt_1 rw,nosuid,nodev,relatime shared:20 - fuse.goclusterfs goclusterfs rw,user_id=1000,group_id=1000\n"
		other = "42 30 0:49 / /tmp/gcfs_mnt_10 rw,nosuid,nodev,relatime shared:21 - fuse.goclusterfs goclusterfs rw,user_id=1000,group_id=1000\n"
	)
	tests := []struct {
		name       string
		mountinfo  string
		mountPoint string
		want       string
	}{
		{"its own mount, not one whose path it prefixes", root + tmp + mnt + other, "/tmp/gcfs_mnt_1", "48"},
		{"the top of stacked mounts", root + tmp + mnt + "43 41 0:52 / /tmp/gcfs_mnt_1 rw - fuse.goclusterfs goclusterfs rw\n", "/tmp/gcfs_mnt_1", "52"},
		{"an escaped path", "44 30 0:53 / /tmp/a\\040b rw - fuse goclusterfs rw\n", "/tmp/a b", "53"},
		{"a major device number", "45 30 8:3 / /tmp/blk rw - fuse.x x rw\n", "/tmp/blk", "8388611"},
		{"no mount", root + tmp + other, "/tmp/gcfs_mnt_1", ""},
		{"a non-FUSE mount on top", root + tmp + mnt + "46 41 0:60 / /tmp/gcfs_mnt_1 rw - tmpfs tmpfs rw\n", "/tmp/gcfs_mnt_1", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := connIDFromMountinfo(tt.mountinfo, tt.mountPoint)
			if got != tt.want || (err != nil) != (tt.want == "") {
				t.Errorf("connIDFromMountinfo = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

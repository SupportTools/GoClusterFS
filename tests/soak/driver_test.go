//go:build linux

package main

import "testing"

// A crash between a link's increment and its name leaves an extra link, a leak repair clears. Too few links or a
// name held twice can lose data, so the soak fails on them.
func TestIsLeakSortsLinkCountProblems(t *testing.T) {
	for line, want := range map[string]bool{
		"inode 12: regular file has 3 links but 2 names: it will never be freed":                     true,
		"inode 12: regular file has 1 links but 2 names: unlinking can free it while a name remains": false,
		`inode 2: holds the name "l0-f1" more than once; a lookup finds only one of them`:            false,
	} {
		if got := isLeak(line); got != want {
			t.Errorf("isLeak(%q) = %v, want %v", line, got, want)
		}
	}
}

//go:build linux

package main

import (
	"slices"
	"testing"
)

// fsck names the first leaked blocks one by one and the rest in one "N more" line, so a kind counts blocks, not lines.
func TestDescribeLeaksCountsEachKind(t *testing.T) {
	leaks := []string{
		"[WARN] Cross-Validation: block 400: leaked: allocated in the bitmap but no inode maps it [FIXABLE]",
		"[WARN] Cross-Validation: blocks: 7 more leaked blocks [FIXABLE]",
		"[WARN] Refcounts: block 401: leaked: no inode maps it but refcount is 1 [FIXABLE]",
		"[WARN] Refcounts: blocks: 3 more leaked blocks have a refcount [FIXABLE]",
		"[WARN] Refcounts: block 402: mapped by 1 inode(s) but refcount is 2: the block will never be freed [FIXABLE]",
		"[WARN] Cross-Validation: inode 12: regular file has 3 links but 2 names: it will never be freed [FIXABLE]",
		"[WARN] Cross-Validation: inode 13: unnamed: no directory names it [FIXABLE]",
		"[WARN] Snapshots: inode 14: snapshot data inode belongs to no snapshot, as a crash while a snapshot is created or deleted leaves it [FIXABLE]",
		"[WARN] Cross-Validation: bitmap: 2 leaked blocks are kept: their bitmap sectors have a bad checksum",
		"[WARN] Cross-Validation: blocks: 8 leaked blocks are kept, because these may map them: node 1's journal",
	}
	want := "18 leaks (bitmap blocks=8, counted blocks=4, extra links=1, high refcounts=1, kept blocks=2, snapshot inodes=1, unnamed inodes=1)"
	if got := describeLeaks(leaks); got != want {
		t.Errorf("describeLeaks = %q\nwant %q", got, want)
	}
	if got := describeLeaks(nil); got != "0 leaks" {
		t.Errorf("describeLeaks(nil) = %q, want 0 leaks", got)
	}
}

// The soak judges fsck by its problem lines, so a CRIT or FATAL line must reach it like a WARN or ERROR one.
func TestProblemLinesKeepsEverySeverityButInfo(t *testing.T) {
	out := []string{
		"Phase 8: Cross-Validation",
		"  [WARN] Cross-Validation: block 400: leaked: allocated in the bitmap but no inode maps it [FIXABLE]",
		"  [INFO] Cross-Validation: node 1: 63 blocks reserved by node 1 and never used; replaying its journal frees them",
		"  [ERROR] Refcounts: block 9: mapped by 2 inodes but refcount is 1: freeing one of them frees a block another still maps",
		"  [CRIT] Refcounts: block 10: refcount overflow (0xFFFFFFFF)",
		"[FATAL] Superblock: superblock: bad magic",
		"Summary: 4 problems",
		"",
	}
	want := []string{
		"[WARN] Cross-Validation: block 400: leaked: allocated in the bitmap but no inode maps it [FIXABLE]",
		"[ERROR] Refcounts: block 9: mapped by 2 inodes but refcount is 1: freeing one of them frees a block another still maps",
		"[CRIT] Refcounts: block 10: refcount overflow (0xFFFFFFFF)",
		"[FATAL] Superblock: superblock: bad magic",
	}
	if got := problemLines(out); !slices.Equal(got, want) {
		t.Errorf("problemLines =\n%q\nwant\n%q", got, want)
	}
}

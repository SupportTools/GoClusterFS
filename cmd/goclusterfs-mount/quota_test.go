package main

import "testing"

// Charges an own-slot replay makes before the journal exists add up per project, and project 0 counts nothing.
func TestLaterChargesAddUpPerProject(t *testing.T) {
	c := laterCharges{}
	c.add(3, -2, -1)
	c.add(3, 1, 0)
	c.add(0, 5, 5)
	if len(c) != 1 || c[3] != [2]int64{-1, -1} {
		t.Errorf("charges = %v, want project 3 at -1 block and -1 inode only", c)
	}
}

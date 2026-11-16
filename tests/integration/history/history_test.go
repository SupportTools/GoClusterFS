package history

import (
	"strings"
	"testing"
	"time"
)

var rules = Rules{Window: time.Second, WriteSize: 12288}

type builder struct{ ops []Op }

func (b *builder) add(o Op, startMs, endMs int) *Op {
	o.ID = len(b.ops)
	o.Start, o.End = time.Duration(startMs)*time.Millisecond, time.Duration(endMs)*time.Millisecond
	b.ops = append(b.ops, o)
	return &b.ops[len(b.ops)-1]
}

func (b *builder) write(name string, v uint64, s, e int) {
	b.add(Op{Kind: Write, Name: name, Value: v}, s, e)
}
func (b *builder) read(name string, v uint64, s, e int) {
	b.add(Op{Kind: Read, Name: name, Value: v, Content: Whole, Node: 1}, s, e)
}
func (b *builder) gone(kind Kind, name string, s, e int) {
	b.add(Op{Kind: kind, Name: name, Result: NotFound, Node: 1}, s, e)
}
func (b *builder) unlink(name string, s, e int) { b.add(Op{Kind: Unlink, Name: name}, s, e) }
func (b *builder) list(s, e int, names ...string) {
	b.add(Op{Kind: Readdir, Names: names, Node: 1}, s, e)
}

func expect(t *testing.T, b *builder, want ...string) {
	t.Helper()
	got := Check(b.ops, rules)
	if len(got) != len(want) {
		for _, v := range got {
			t.Log(v)
		}
		t.Fatalf("%d violations, want %d %q", len(got), len(want), want)
	}
	for i, v := range got {
		if !strings.Contains(v.What, want[i]) {
			t.Errorf("violation %d is %q, want %q\n%s", i, v.What, want[i], v)
		}
	}
}

func TestConcurrentHistoryPasses(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.write("a", 2, 100, 200)
	b.read("a", 1, 150, 160)
	b.read("a", 2, 150, 160)
	b.read("a", 2, 300, 310)
	b.add(Op{Kind: Stat, Name: "a", Size: 12288}, 300, 301)
	b.unlink("a", 400, 410)
	b.read("a", 2, 500, 510)
	b.gone(Read, "a", 500, 510)
	b.gone(Stat, "a", 1500, 1510)
	b.list(1500, 1510)
	b.add(Op{Kind: Create, Name: "b"}, 1600, 1610)
	b.add(Op{Kind: Read, Name: "b", Content: Empty}, 1700, 1710)
	b.add(Op{Kind: Create, Name: "b", Result: Exists}, 1700, 1710)
	b.list(1700, 1710)
	b.list(2700, 2710, "b")
	expect(t, b)
}

func TestStaleReadRightAfterOpen(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.write("a", 2, 2000, 2010)
	b.read("a", 1, 2050, 2060)
	expect(t, b, "stale or unknown value right after open")
}

func TestNameChangesMayLagTheWindow(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.unlink("a", 2000, 2010)
	b.write("a", 2, 2020, 2030)
	b.gone(Read, "a", 2100, 2110)
	b.read("a", 1, 2100, 2110)
	b.gone(Read, "a", 3100, 3110)
	expect(t, b, "read missed a name that should exist")
}

func TestTornRead(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.add(Op{Kind: Read, Name: "a", Content: Torn, Size: 12288, Pieces: []Piece{{0, 1}, {968, 2}}}, 20, 30)
	expect(t, b, "torn read")
}

func TestSectorTearDuringALocalWriteIsAllowed(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.add(Op{Kind: Write, Name: "a", Value: 2, Node: 1}, 20, 40)
	tear := Op{Kind: Read, Name: "a", Content: Torn, Size: 12288, Node: 1, Pieces: []Piece{{0, 1}, {4096, 2}}}
	b.add(tear, 25, 30)
	b.add(Op{Kind: Write, Name: "b", Value: 3, Node: 0}, 20, 40)
	b.add(Op{Kind: Read, Name: "b", Content: Torn, Size: 12288, Node: 1, Pieces: []Piece{{0, 0}, {4096, 3}}}, 25, 30)
	tear.Node = 0
	b.add(tear, 25, 30)
	expect(t, b, "torn read", "torn read")
}

func TestUnknownValue(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.read("a", 7, 20, 30)
	expect(t, b, "stale or unknown value")
}

func TestResurrectedName(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.unlink("a", 20, 30)
	b.add(Op{Kind: Stat, Name: "a", Size: 12288}, 2000, 2010)
	b.list(2000, 2010, "a")
	b.add(Op{Kind: Create, Name: "a", Result: Exists}, 2000, 2010)
	expect(t, b, "stat found a name that should be gone", "listing shows a name that should be gone (a)", "create found a name that should be gone")
}

func TestListingMissesCreatedName(t *testing.T) {
	b := &builder{}
	b.add(Op{Kind: Create, Name: "a"}, 0, 10)
	b.list(2000, 2010)
	b.gone(Unlink, "a", 2000, 2010)
	expect(t, b, "listing misses a name that should exist (a)", "unlink missed a name that should exist")
}

func TestFailedCallsMayOrMayNotTakeEffect(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.add(Op{Kind: Write, Name: "a", Value: 2, Result: Failed}, 100, 110)
	b.read("a", 1, 2000, 2010)
	b.read("a", 2, 2000, 2010)
	b.add(Op{Kind: Unlink, Name: "b", Result: NotFound}, 100, 110)
	b.add(Op{Kind: Rename, Name: "a", To: "c", Result: Unsupported}, 100, 110)
	b.list(3000, 3010, "a")
	expect(t, b, "write failed")
}

func TestFailedCallSupersededByALaterOne(t *testing.T) {
	b := &builder{}
	b.add(Op{Kind: Write, Name: "a", Value: 2, Result: Failed}, 0, 10)
	b.write("a", 1, 100, 110)
	b.read("a", 2, 2000, 2010)
	expect(t, b, "write failed", "stale or unknown value right after open")
}

func TestCreatingWritePassesThroughEmpty(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 100, 110)
	b.add(Op{Kind: Stat, Name: "a"}, 105, 106)
	b.add(Op{Kind: Read, Name: "a", Content: Empty}, 105, 106)
	b.write("a", 2, 2000, 2010)
	b.add(Op{Kind: Read, Name: "a", Content: Empty}, 2005, 2006)
	expect(t, b, "read saw a state no order explains right after open")
}

func TestNonExclusiveOpenNeverFailsWithEEXIST(t *testing.T) {
	b := &builder{}
	b.add(Op{Kind: Write, Name: "a", Value: 1, Result: Exists}, 0, 10)
	expect(t, b, "open with O_CREAT and no O_EXCL failed with EEXIST")
}

func TestRenameShowsExactlyOneName(t *testing.T) {
	b := &builder{}
	b.write("a", 1, 0, 10)
	b.add(Op{Kind: Rename, Name: "a", To: "b"}, 2000, 2010)
	b.list(2005, 2015, "a", "b")
	b.list(2005, 2015, "b")
	expect(t, b, "listing shows both names of a rename")
}

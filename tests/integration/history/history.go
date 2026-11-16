// Package history checks a record of concurrent file calls from several nodes
// against GoClusterFS's documented cross-node guarantees.
package history

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

type Kind uint8

const (
	Create  Kind = iota // open with O_CREAT|O_EXCL, then close
	Write               // open with O_CREAT, write one whole value at offset 0, close
	Read                // open, read the whole file, close
	Stat                //
	Readdir             // list the directory that holds every name
	Unlink              //
	Rename              // Name to To
)

func (k Kind) String() string {
	return [...]string{"create", "write", "read", "stat", "readdir", "unlink", "rename"}[k]
}

type Result uint8

const (
	OK          Result = iota
	NotFound           // ENOENT
	Exists             // EEXIST
	Unsupported        // the call is not implemented; it changed nothing
	Failed             // any other error: the call may or may not have taken effect
	Stale              // open failed with ESTALE; it changed nothing
)

func (r Result) String() string {
	return [...]string{"ok", "ENOENT", "EEXIST", "unsupported", "failed", "ESTALE"}[r]
}

// Content is what a successful Read saw.
type Content uint8

const (
	Empty Content = iota // no bytes
	Whole                // exactly one value, Value, over the whole write size
	Torn                 // anything else: two values, zeros, or a short file
)

// Op is one call, timed by a clock all clients share.
type Op struct {
	ID       int // unique within a history
	Client   int
	Node     int
	Kind     Kind
	Name, To string
	Start    time.Duration
	End      time.Duration
	Result   Result
	Err      string
	Value    uint64   // Write: the value written. Read: the value seen, when Content is Whole.
	Pieces   []Piece  // Read: each run of one value, when Content is Torn
	Content  Content  // Read
	Size     int64    // Stat
	Names    []string // Readdir
}

func (o Op) String() string {
	s := fmt.Sprintf("[%9.3fs %9.3fs] node %d client %d %s %s", o.Start.Seconds(), o.End.Seconds(), o.Node, o.Client, o.Kind, o.Name)
	if o.Kind == Rename {
		s += " -> " + o.To
	}
	switch {
	case o.Result != OK:
		s += " = " + o.Result.String()
		if o.Err != "" {
			s += " (" + o.Err + ")"
		}
	case o.Kind == Write:
		s += fmt.Sprintf(" %#x", o.Value)
	case o.Kind == Read && o.Content == Whole:
		s += fmt.Sprintf(" = %#x", o.Value)
	case o.Kind == Read && o.Content == Torn:
		s += fmt.Sprintf(" = torn: %d bytes", o.Size)
		for _, p := range o.Pieces {
			s += fmt.Sprintf(", %#x from %d", p.Value, p.At)
		}
	case o.Kind == Read:
		s += " = empty"
	case o.Kind == Stat:
		s += fmt.Sprintf(" = size %d", o.Size)
	case o.Kind == Readdir:
		s += " = [" + strings.Join(o.Names, " ") + "]"
	}
	return s
}

// Piece is a run of one value in a torn read, starting at byte At.
type Piece struct {
	At    int64
	Value uint64
}

// Rules are the guarantees a history is checked against.
type Rules struct {
	// Window is how long stat, lookups and listings on one node may show a name
	// as it was before another node's change: -attr-timeout plus some slack.
	Window time.Duration
	// WriteSize is the size of every Write. Stat sizes must be 0 or WriteSize.
	WriteSize int64
}

// Violation is an observation no allowed order of the history explains.
type Violation struct {
	What    string
	Op      Op
	Allowed string
	History []Op // the observation and every mutation that decides what it may see
}

func (v Violation) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %s\n  allowed: %s\n  history:\n", v.What, v.Op, v.Allowed)
	for _, o := range v.History {
		fmt.Fprintf(&b, "    %s\n", o)
	}
	return b.String()
}

// state is one possible content of a name.
type state struct {
	present bool
	empty   bool
	any     bool // a rename moved some content here
	value   uint64
}

func (s state) String() string {
	switch {
	case !s.present:
		return "absent"
	case s.any:
		return "present"
	case s.empty:
		return "empty"
	}
	return fmt.Sprintf("%#x", s.value)
}

// mutation is an Op's effect on one name.
type mutation struct {
	op    Op
	after state
	maybe bool // the call failed, so it may not have taken effect
}

type nameLog struct {
	byEnd    []mutation // sorted by op.End
	maxStart []time.Duration
	maybes   []mutation
	maxDur   time.Duration
}

// Check returns every observation in ops that violates r.
func Check(ops []Op, r Rules) []Violation {
	logs := map[string]*nameLog{}
	add := func(name string, m mutation) {
		l := logs[name]
		if l == nil {
			l = &nameLog{}
			logs[name] = l
		}
		if m.maybe {
			l.maybes = append(l.maybes, m)
		} else {
			l.byEnd = append(l.byEnd, m)
		}
		l.maxDur = max(l.maxDur, m.op.End-m.op.Start)
	}
	for _, o := range ops {
		maybe := o.Result == Failed
		if o.Result != OK && !maybe {
			continue
		}
		switch o.Kind {
		case Create:
			add(o.Name, mutation{o, state{present: true, empty: true}, maybe})
		case Write:
			add(o.Name, mutation{o, state{present: true, value: o.Value}, maybe})
		case Unlink:
			add(o.Name, mutation{o, state{}, maybe})
		case Rename:
			add(o.Name, mutation{o, state{}, maybe})
			add(o.To, mutation{o, state{present: true, any: true}, maybe})
		}
	}
	for _, l := range logs {
		sort.Slice(l.byEnd, func(i, j int) bool { return l.byEnd[i].op.End < l.byEnd[j].op.End })
		l.maxStart = make([]time.Duration, len(l.byEnd))
		for i, m := range l.byEnd {
			l.maxStart[i] = m.op.Start
			if i > 0 {
				l.maxStart[i] = max(l.maxStart[i], l.maxStart[i-1])
			}
		}
	}

	var out []Violation
	for _, o := range ops {
		switch {
		case o.Result == Failed:
			out = append(out, Violation{What: o.Kind.String() + " failed", Op: o, Allowed: "success, ENOENT or EEXIST", History: []Op{o}})
		case o.Result == Stale && !churned(o, logs[o.Name], r):
			out = append(out, Violation{What: "open failed with ESTALE while its name kept its file", Op: o, Allowed: "ESTALE only when the name lost its file twice within the cache window and the call, once during the call", History: []Op{o}})
		case o.Kind == Write && o.Result == Exists:
			out = append(out, Violation{What: "open with O_CREAT and no O_EXCL failed with EEXIST", Op: o, Allowed: "the file opened", History: []Op{o}})
		case o.Result != OK && o.Result != NotFound && o.Result != Exists:
		case o.Kind == Read && o.Content == Torn && localWriteTear(o, logs[o.Name], r):
		case o.Kind == Read || o.Kind == Stat:
			if v, bad := checkName(o, o.Name, observed(o, r), logs[o.Name], r); bad {
				out = append(out, v)
			}
		case o.Kind == Unlink || o.Kind == Create:
			if v, bad := checkName(o, o.Name, observed(o, r), logs[o.Name], r); bad {
				out = append(out, v)
			}
		case o.Kind == Readdir:
			for name, l := range logs {
				listed := slices.Contains(o.Names, name)
				if v, bad := checkName(o, name, func(s state) bool { return s.present == listed }, l, r); bad {
					v.What = map[bool]string{true: "listing shows a name that should be gone", false: "listing misses a name that should exist"}[listed] + " (" + name + ")"
					out = append(out, v)
				}
			}
		}
	}
	return append(out, checkRenames(ops, logs, r)...)
}

// observed returns which states explain what o saw of its name.
func observed(o Op, r Rules) func(state) bool {
	switch {
	case o.Kind == Read && o.Result == NotFound, o.Kind == Stat && o.Result == NotFound,
		o.Kind == Unlink && o.Result == NotFound, o.Kind == Create && o.Result == OK:
		return func(s state) bool { return !s.present }
	case o.Kind == Unlink && o.Result == OK, o.Kind == Create && o.Result == Exists:
		return func(s state) bool { return s.present }
	case o.Kind == Read && o.Content == Torn:
		return func(state) bool { return false }
	case o.Kind == Read && o.Content == Empty:
		return func(s state) bool { return s.present && (s.empty || s.any) }
	case o.Kind == Read:
		return func(s state) bool { return s.present && (s.any || !s.empty && s.value == o.Value) }
	case o.Kind == Stat && o.Size == 0:
		return func(s state) bool { return s.present && (s.empty || s.any) }
	case o.Kind == Stat && o.Size == r.WriteSize:
		return func(s state) bool { return s.present && (s.any || !s.empty) }
	}
	return func(state) bool { return false }
}

// checkName finds the states o may see of name and reports a violation when seen accepts none of them.
func checkName(o Op, name string, seen func(state) bool, l *nameLog, r Rules) (Violation, bool) {
	window := r.Window
	if o.Kind == Read && stableName(o, l, r) {
		window = 0
	}
	allowed, deciding := possible(o, l, o.Start-window)
	for _, s := range allowed {
		if seen(s) {
			return Violation{}, false
		}
	}
	v := Violation{Op: o, What: describe(o, allowed, window == 0, r), History: append(deciding, o)}
	var names []string
	for _, s := range allowed {
		names = append(names, s.String())
	}
	v.Allowed = strings.Join(names, " or ")
	if o.Kind == Read && o.Content == Whole && l != nil {
		for _, m := range l.byEnd {
			if m.op.Kind == Write && m.op.Value == o.Value && !slices.ContainsFunc(v.History, func(h Op) bool { return h.ID == m.op.ID }) {
				v.History = append(v.History, m.op)
			}
		}
	}
	sort.SliceStable(v.History, func(i, j int) bool { return v.History[i].Start < v.History[j].Start })
	return v, true
}

// possible returns the states o may see when changes that ended before settled
// must show, with the mutations that decide them. o's own effect is excluded.
func possible(o Op, l *nameLog, settled time.Duration) ([]state, []Op) {
	if l == nil {
		return []state{{}}, nil
	}
	var states []state
	var deciding []Op
	take := func(m mutation) {
		if m.op.ID == o.ID {
			return
		}
		if !slices.Contains(states, m.after) {
			states = append(states, m.after)
		}
		deciding = append(deciding, m.op)
	}
	n := sort.Search(len(l.byEnd), func(i int) bool { return l.byEnd[i].op.End >= settled })
	latest := time.Duration(-1 << 62)
	if n == 0 {
		states = append(states, state{})
	} else {
		latest = l.maxStart[n-1]
		for i := sort.Search(n, func(i int) bool { return l.byEnd[i].op.End >= latest }); i < n; i++ {
			take(l.byEnd[i])
		}
		if len(states) == 0 {
			states = append(states, state{})
		}
	}
	writing := false
	l.during(settled, o.End, func(m mutation) {
		take(m)
		writing = writing || m.op.Kind == Write && m.op.ID != o.ID
	})
	for _, m := range l.maybes {
		if m.op.Start < o.End && m.op.End >= latest {
			take(m)
			writing = writing || m.op.Kind == Write
		}
	}
	created := state{present: true, empty: true}
	if writing && slices.Contains(states, state{}) && !slices.Contains(states, created) {
		states = append(states, created)
	}
	return states, deciding
}

// during calls fn for each sure mutation that ended at or after from and started before to.
func (l *nameLog) during(from, to time.Duration, fn func(mutation)) {
	for i := sort.Search(len(l.byEnd), func(i int) bool { return l.byEnd[i].op.End >= from }); i < len(l.byEnd) && l.byEnd[i].op.End <= to+l.maxDur; i++ {
		if l.byEnd[i].op.Start < to {
			fn(l.byEnd[i])
		}
	}
}

// churned reports whether o's name lost its file twice: once while the kernel may still cache the old entry, and again during o after the retry's lookup.
func churned(o Op, l *nameLog, r Rules) bool {
	if l == nil {
		return false
	}
	before, during := 0, 0
	l.during(o.Start-r.Window, o.End, func(m mutation) {
		switch {
		case m.after.present:
		case m.op.End >= o.Start:
			during++
		default:
			before++
		}
	})
	return during >= 1 && before+during >= 2
}

// localWriteTear reports whether a torn read overlapped a write on its own node and, apart from words the page cache split, saw only values it may see.
func localWriteTear(o Op, l *nameLog, r Rules) bool {
	if o.Size != r.WriteSize || len(o.Pieces) < 2 || l == nil {
		return false
	}
	allowed, _ := possible(o, l, o.Start-r.Window)
	overlapping := false
	for _, p := range wholeWords(o.Pieces) {
		if p.At%8 != 0 || p.Value == 0 || !slices.Contains(allowed, state{present: true, value: p.Value}) {
			return false
		}
		l.during(o.Start, o.End, func(m mutation) {
			overlapping = overlapping || m.op.Kind == Write && m.op.Node == o.Node && m.op.Value == p.Value
		})
	}
	return overlapping
}

// wholeWords drops each 8-byte piece whose bytes all come from the pieces either side: the page cache can split a copy inside a word.
func wholeWords(pieces []Piece) []Piece {
	var out []Piece
	for i, p := range pieces {
		if i == 0 || i == len(pieces)-1 || pieces[i+1].At-p.At != 8 || !byteMix(p.Value, pieces[i-1].Value, pieces[i+1].Value) {
			out = append(out, p)
		}
	}
	return out
}

func byteMix(v, a, b uint64) bool {
	for shift := 0; shift < 64; shift += 8 {
		if x := v >> shift & 0xff; x != a>>shift&0xff && x != b>>shift&0xff {
			return false
		}
	}
	return true
}

// stableName reports whether name kept one file for a whole window before o
// and during it: then o's open finds that file, and must see its completed writes.
func stableName(o Op, l *nameLog, r Rules) bool {
	if l == nil {
		return false
	}
	before, _ := possible(o, l, o.Start-r.Window)
	if slices.ContainsFunc(before, func(s state) bool { return !s.present }) {
		return false
	}
	moved := false
	l.during(o.Start-r.Window, o.End, func(m mutation) { moved = moved || m.op.Kind != Write })
	return !moved && !slices.ContainsFunc(l.maybes, func(m mutation) bool {
		return m.op.Kind != Write && m.op.End >= o.Start-r.Window && m.op.Start < o.End
	})
}

func describe(o Op, allowed []state, strict bool, r Rules) string {
	present := slices.ContainsFunc(allowed, func(s state) bool { return s.present })
	absent := slices.ContainsFunc(allowed, func(s state) bool { return !s.present })
	suffix := ""
	if strict {
		suffix = " right after open"
	}
	switch {
	case o.Kind == Read && o.Result == OK && o.Content == Torn:
		return "torn read"
	case o.Kind == Stat && o.Result == OK && o.Size != 0 && o.Size != r.WriteSize:
		return fmt.Sprintf("stat size %d is no write's size", o.Size)
	case !present && o.Result == OK && (o.Kind == Read || o.Kind == Stat || o.Kind == Unlink),
		!present && o.Kind == Create && o.Result == Exists:
		return o.Kind.String() + " found a name that should be gone"
	case !absent && (o.Result == NotFound || o.Kind == Create && o.Result == OK):
		return o.Kind.String() + " missed a name that should exist" + suffix
	case o.Kind == Read && o.Content == Whole:
		return "stale or unknown value" + suffix
	}
	return o.Kind.String() + " saw a state no order explains" + suffix
}

// checkRenames checks that a rename onto a free name, with no other change to
// either name nearby, never shows in a listing as both names or neither.
func checkRenames(ops []Op, logs map[string]*nameLog, r Rules) []Violation {
	var listings []Op
	var longest time.Duration
	for _, o := range ops {
		if o.Kind == Readdir && o.Result == OK {
			listings = append(listings, o)
			longest = max(longest, o.End-o.Start)
		}
	}
	sort.Slice(listings, func(i, j int) bool { return listings[i].Start < listings[j].Start })
	var out []Violation
	for _, rn := range ops {
		if rn.Kind != Rename || rn.Result != OK || !quiet(rn, logs[rn.Name], r) || !quiet(rn, logs[rn.To], r) {
			continue
		}
		if before, _ := possible(rn, logs[rn.To], rn.Start-r.Window); slices.ContainsFunc(before, func(s state) bool { return s.present }) {
			continue
		}
		for i := sort.Search(len(listings), func(i int) bool { return listings[i].Start >= rn.Start-longest }); i < len(listings) && listings[i].Start <= rn.End+r.Window; i++ {
			d := listings[i]
			if d.End < rn.Start {
				continue
			}
			from, to := slices.Contains(d.Names, rn.Name), slices.Contains(d.Names, rn.To)
			if from == to {
				out = append(out, Violation{
					What:    map[bool]string{true: "listing shows both names of a rename", false: "listing shows neither name of a rename"}[from],
					Op:      d,
					Allowed: "exactly one of " + rn.Name + ", " + rn.To,
					History: []Op{rn, d},
				})
			}
		}
	}
	return out
}

// quiet reports whether nothing but rn changed the name logged in l within a window of rn.
func quiet(rn Op, l *nameLog, r Rules) bool {
	if l == nil {
		return true
	}
	busy := false
	l.during(rn.Start-r.Window, rn.End+r.Window, func(m mutation) { busy = busy || m.op.ID != rn.ID })
	return !busy && !slices.ContainsFunc(l.maybes, func(m mutation) bool {
		return m.op.ID != rn.ID && m.op.End >= rn.Start-r.Window && m.op.Start < rn.End+r.Window
	})
}

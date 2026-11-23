package journal

import (
	"encoding/binary"
	"fmt"
	"log"
	"maps"
	"slices"
)

// QuotaDeltaPayload is this node's change to a project's usage. The node's ledger holds it until a flush moves it into
// the quota table; until then the journal keeps the record, so a replay can fold it in.
type QuotaDeltaPayload struct {
	Project uint32
	Blocks  int64
	Inodes  int64
}

func (p *QuotaDeltaPayload) Serialize() []byte {
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:4], p.Project)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(p.Blocks))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(p.Inodes))
	return buf
}

func DeserializeQuotaDelta(buf []byte) (*QuotaDeltaPayload, error) {
	if len(buf) < 24 {
		return nil, ErrTxnTooSmall
	}
	return &QuotaDeltaPayload{
		Project: binary.LittleEndian.Uint32(buf[0:4]),
		Blocks:  int64(binary.LittleEndian.Uint64(buf[8:16])),
		Inodes:  int64(binary.LittleEndian.Uint64(buf[16:24])),
	}, nil
}

// QuotaUpdatePayload changes a project's usage in the quota table from Old to New. A QuotaFlush also moves Covered,
// all of this node's ledger for the project, out of the ledger. A QuotaFold is what a replay of the journal that holds
// it wrote, and leaves Covered zero.
type QuotaUpdatePayload struct {
	Project       uint32
	CoveredBlocks int64
	CoveredInodes int64
	OldBlocks     uint64
	NewBlocks     uint64
	OldInodes     uint64
	NewInodes     uint64
}

func (p *QuotaUpdatePayload) Serialize() []byte {
	buf := make([]byte, 56)
	binary.LittleEndian.PutUint32(buf[0:4], p.Project)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(p.CoveredBlocks))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(p.CoveredInodes))
	binary.LittleEndian.PutUint64(buf[24:32], p.OldBlocks)
	binary.LittleEndian.PutUint64(buf[32:40], p.NewBlocks)
	binary.LittleEndian.PutUint64(buf[40:48], p.OldInodes)
	binary.LittleEndian.PutUint64(buf[48:56], p.NewInodes)
	return buf
}

func DeserializeQuotaUpdate(buf []byte) (*QuotaUpdatePayload, error) {
	if len(buf) < 56 {
		return nil, ErrTxnTooSmall
	}
	return &QuotaUpdatePayload{
		Project:       binary.LittleEndian.Uint32(buf[0:4]),
		CoveredBlocks: int64(binary.LittleEndian.Uint64(buf[8:16])),
		CoveredInodes: int64(binary.LittleEndian.Uint64(buf[16:24])),
		OldBlocks:     binary.LittleEndian.Uint64(buf[24:32]),
		NewBlocks:     binary.LittleEndian.Uint64(buf[32:40]),
		OldInodes:     binary.LittleEndian.Uint64(buf[40:48]),
		NewInodes:     binary.LittleEndian.Uint64(buf[48:56]),
	}, nil
}

func (txn *Transaction) LogQuotaDelta(p *QuotaDeltaPayload) error {
	if err := txn.addEntry(TxnOpQuotaDelta, p.Serialize()); err != nil {
		return err
	}
	txn.quota = append(txn.quota, quotaChange{project: p.Project, blocks: p.Blocks, inodes: p.Inodes})
	return nil
}

func (txn *Transaction) LogQuotaFlush(p *QuotaUpdatePayload) error {
	if err := txn.addEntry(TxnOpQuotaFlush, p.Serialize()); err != nil {
		return err
	}
	txn.quota = append(txn.quota, quotaChange{project: p.Project, blocks: -p.CoveredBlocks, inodes: -p.CoveredInodes, flush: true})
	return nil
}

func (txn *Transaction) LogQuotaFold(p *QuotaUpdatePayload) error {
	return txn.addEntry(TxnOpQuotaFold, p.Serialize())
}

type quotaChange struct {
	project        uint32
	blocks, inodes int64
	flush          bool
}

type quotaHold struct {
	seq   uint64
	start uint32
}

// quotaLedger is this node's usage changes per project that no flush moved into the quota table yet. Commits change
// it under the journal's lock, in commit order, so a flush covers exactly the deltas committed before it. Each
// project with changes a durable flush has not covered holds the journal's head at its first such delta, so a replay
// still finds them.
type quotaLedger struct {
	pending map[uint32]QuotaDeltaPayload
	holds   map[uint32]quotaHold
	// after is the first delta committed after a project's flush, which holds the head once that flush is durable.
	after    map[uint32]quotaHold
	flushing map[uint32]bool
}

func newQuotaLedger() quotaLedger {
	return quotaLedger{pending: map[uint32]QuotaDeltaPayload{}, holds: map[uint32]quotaHold{},
		after: map[uint32]quotaHold{}, flushing: map[uint32]bool{}}
}

func (l *quotaLedger) pendingFor(project uint32) QuotaDeltaPayload {
	p := l.pending[project]
	p.Project = project
	return p
}

// apply adds a committed transaction's changes, or with sign -1 takes back those of one whose commit failed. Taking
// back a delta keeps its hold: an earlier flush may have counted it.
func (l *quotaLedger) apply(seq uint64, start uint32, changes []quotaChange, sign int64) {
	at := quotaHold{seq: seq, start: start}
	for _, c := range changes {
		p := l.pending[c.project]
		p.Blocks += sign * c.blocks
		p.Inodes += sign * c.inodes
		l.pending[c.project] = p
		switch {
		case c.flush && sign > 0:
			l.flushing[c.project] = true
			delete(l.after, c.project)
		case c.flush:
			delete(l.flushing, c.project)
			delete(l.after, c.project)
		case sign > 0:
			if _, held := l.holds[c.project]; !held {
				l.holds[c.project] = at
			}
			if _, set := l.after[c.project]; l.flushing[c.project] && !set {
				l.after[c.project] = at
			}
		}
	}
}

// settle moves the holds of the projects a durable flush covered to their first delta after it, and forgets the
// projects it left with nothing pending.
func (l *quotaLedger) settle(changes []quotaChange) {
	for _, c := range changes {
		if !c.flush {
			continue
		}
		next, moved := l.after[c.project]
		delete(l.flushing, c.project)
		delete(l.after, c.project)
		switch p := l.pending[c.project]; {
		case p.Blocks == 0 && p.Inodes == 0:
			delete(l.pending, c.project)
			delete(l.holds, c.project)
		case moved:
			l.holds[c.project] = next
		}
	}
}

// QuotaPending returns this node's usage changes per project that no flush moved into the quota table yet.
func (j *Journal) QuotaPending() map[uint32]QuotaDeltaPayload {
	j.mu.Lock()
	defer j.mu.Unlock()
	return maps.Clone(j.quota.pending)
}

// QuotaReplay is what a dirty journal's quota records still owe the quota table. The replaying node folds it in
// through the journal itself before replay begins: it appends one transaction of QuotaFold records with
// AppendQuotaFold, then writes the usage. A fold in the journal stands for every quota record before it, so a fold
// that stopped part way is redone from the fold alone, and only its writes that did not land change. Applying the
// records one by one instead could not tell a usage value that came back to an old one from one never written.
type QuotaReplay struct {
	// Running is, per project, the sum of the deltas logged after its last flush.
	Running map[uint32]QuotaDeltaPayload
	// Updates are the flushes and folds whose intents did not complete, in journal order: their writes may not have
	// landed.
	Updates []QuotaUpdatePayload
	// Folded is set when the journal holds a fold already, so its changes need no new one, or replay began, which it
	// does only after the fold.
	Folded bool
}

// ReadQuotaReplay reads the quota records of the journal in region, completed or not. Records of transactions whose
// commit failed do not count.
func ReadQuotaReplay(region *Region) (*QuotaReplay, error) {
	q := &QuotaReplay{Running: map[uint32]QuotaDeltaPayload{}}
	switch region.State() {
	case JournalStateDirty:
	case JournalStateRecovering:
		q.Folded = true
		return q, nil
	default:
		return q, nil
	}
	err := scanJournal(region, func(txn *RecoveredTxn) error {
		if txn.Abandoned {
			return nil
		}
		if slices.ContainsFunc(txn.Entries, func(e *RecoveredEntry) bool { return e.Entry.OpType == TxnOpQuotaFold }) {
			clear(q.Running)
			q.Updates, q.Folded = nil, true
		}
		for _, e := range txn.Entries {
			if err := q.add(e, txn.Completed); err != nil {
				return fmt.Errorf("txn %d: %w", txn.Header.Seq, err)
			}
		}
		return nil
	}, &RecoveryStats{}, nil, nil, true)
	if err != nil {
		return nil, err
	}
	maps.DeleteFunc(q.Running, func(_ uint32, d QuotaDeltaPayload) bool { return d.Blocks == 0 && d.Inodes == 0 })
	return q, nil
}

func (q *QuotaReplay) add(e *RecoveredEntry, completed bool) error {
	switch e.Entry.OpType {
	case TxnOpQuotaDelta:
		p, err := DeserializeQuotaDelta(e.Payload)
		if err != nil {
			return err
		}
		r := q.Running[p.Project]
		r.Project = p.Project
		r.Blocks += p.Blocks
		r.Inodes += p.Inodes
		q.Running[p.Project] = r
	case TxnOpQuotaFlush, TxnOpQuotaFold:
		p, err := DeserializeQuotaUpdate(e.Payload)
		if err != nil {
			return err
		}
		if e.Entry.OpType == TxnOpQuotaFlush {
			delete(q.Running, p.Project)
		}
		if !completed {
			q.Updates = append(q.Updates, *p)
		}
	}
	return nil
}

// Fold returns the usage changes that bring the table up to date with the journal, with Old set to what used reads
// now. Each update applies only while the usage still holds its Old, since otherwise it landed, and then the running
// deltas add on. Usage that would go below zero stops at zero.
func (q *QuotaReplay) Fold(used func(project uint32) (blocks, inodes uint64, err error)) ([]QuotaUpdatePayload, error) {
	projects := slices.Collect(maps.Keys(q.Running))
	for _, u := range q.Updates {
		projects = append(projects, u.Project)
	}
	slices.Sort(projects)
	var folds []QuotaUpdatePayload
	for _, project := range slices.Compact(projects) {
		blocks, inodes, err := used(project)
		if err != nil {
			return nil, fmt.Errorf("project %d: %w", project, err)
		}
		f := QuotaUpdatePayload{Project: project, OldBlocks: blocks, OldInodes: inodes, NewBlocks: blocks, NewInodes: inodes}
		for _, u := range q.Updates {
			if u.Project == project && f.NewBlocks == u.OldBlocks && f.NewInodes == u.OldInodes {
				f.NewBlocks, f.NewInodes = u.NewBlocks, u.NewInodes
			}
		}
		r := q.Running[project]
		f.NewBlocks = addClamped(f.NewBlocks, r.Blocks, project, "blocks")
		f.NewInodes = addClamped(f.NewInodes, r.Inodes, project, "inodes")
		if f.NewBlocks != f.OldBlocks || f.NewInodes != f.OldInodes {
			folds = append(folds, f)
		}
	}
	return folds, nil
}

func addClamped(used uint64, delta int64, project uint32, what string) uint64 {
	if delta < 0 && uint64(-delta) > used {
		log.Printf("[WARN] quota: project %d would use %d %s less than its %d; counting 0 until goclusterfs-fsck -repair", project, -delta, what, used)
		return 0
	}
	return uint64(int64(used) + delta)
}

// AppendQuotaFold appends folds as one transaction to the dirty journal in region, which no node has open, and makes
// it durable before the usage is written.
func AppendQuotaFold(region *Region, folds []QuotaUpdatePayload) error {
	sb := region.Superblock()
	txn := NewTransaction(0, sb.NodeUUID, sb.NodeGeneration)
	for i := range folds {
		if err := txn.LogQuotaFold(&folds[i]); err != nil {
			return err
		}
	}
	if _, _, _, err := region.Append(func(seq uint64) ([]byte, error) {
		txn.setSeq(seq)
		return txn.Serialize()
	}); err != nil {
		return fmt.Errorf("appending the quota fold: %w", err)
	}
	return syncAppended(region)
}

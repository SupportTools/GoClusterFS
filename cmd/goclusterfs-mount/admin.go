package main

import (
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/api"
	"github.com/supporttools/GoClusterFS/internal/cache"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/trim"
)

// adminOps implements api.AdminService for this node. journal and trim are nil when their flags turn them off.
type adminOps struct {
	journal *journal.Journal
	trim    *trim.Manager
	cache   *cache.Manager
}

func (a adminOps) FlushJournal() (string, error) {
	if a.journal == nil {
		return "", fmt.Errorf("the journal is %w (-journal=false)", api.ErrDisabled)
	}
	done, err := a.journal.Checkpoint()
	switch {
	case err != nil:
		return "", err
	case !done:
		return "nothing to checkpoint, or a transaction is still active", nil
	default:
		return "journal checkpointed", nil
	}
}

func (a adminOps) ForceTrim() (string, error) {
	if a.trim == nil {
		return "", fmt.Errorf("TRIM is %w (-trim=false)", api.ErrDisabled)
	}
	return fmt.Sprintf("sent %d queued blocks to discard", a.trim.Flush()), nil
}

func (a adminOps) ClearCache() (string, error) {
	a.cache.Clear()
	return "inode, extent and negative caches cleared", nil
}

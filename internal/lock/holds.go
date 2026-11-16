package lock

import (
	"fmt"
	"log"
	"maps"
	"slices"
	"strings"
	"time"
)

// longHold is how long a node holds a record before it warns that the hold may
// have leaked. Under an owner view a claim never goes stale, so a hold that is
// never released lasts until unmount.
var longHold = time.Minute

// releaseRetry is how often the watcher clears again a slot whose release write
// failed. Other nodes wait for the record until it is cleared.
var releaseRetry = time.Second

// Start runs a watcher that logs a warning for each record this node has held
// for more than longHold. Stop ends it.
func (m *Manager) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopWatch != nil {
		return
	}
	m.stopWatch, m.watchDone = make(chan struct{}), make(chan struct{})
	go m.watchHolds(m.stopWatch, m.watchDone)
}

// Stop ends the watcher and waits for it to finish.
func (m *Manager) Stop() {
	m.mu.Lock()
	stop, done := m.stopWatch, m.watchDone
	m.stopWatch, m.watchDone = nil, nil
	m.mu.Unlock()
	if stop != nil {
		close(stop)
		<-done
	}
}

func (m *Manager) watchHolds(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	tick := time.NewTicker(longHold / 6)
	defer tick.Stop()
	retry := time.NewTicker(releaseRetry)
	defer retry.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			m.warnLongHolds()
		case <-retry.C:
			m.retryReleases()
		}
	}
}

// warnLongHolds logs each record held for more than longHold once, with the
// keys and goroutines that hold it.
func (m *Manager) warnLongHolds() {
	m.mu.Lock()
	var warnings []string
	for recordID, rec := range m.records {
		if rec.keys == 0 || rec.warnedLong || age(rec.since) <= longHold {
			continue
		}
		rec.warnedLong = true
		var holders []string
		for key, held := range m.heldLocks {
			if held.RecordID == recordID {
				holders = append(holders, fmt.Sprintf("type=%d res=%d mode=%d goroutines %v", key.Type, key.ResourceID, held.Mode, slices.Sorted(maps.Keys(held.goroutines))))
			}
		}
		slices.Sort(holders)
		warnings = append(warnings, fmt.Sprintf("[WARN] lock: this node has held record %d for %v (%s); other nodes wait for it until it is released",
			recordID, age(rec.since).Round(time.Second), strings.Join(holders, "; ")))
	}
	m.mu.Unlock()
	for _, w := range warnings {
		log.Print(w)
	}
}

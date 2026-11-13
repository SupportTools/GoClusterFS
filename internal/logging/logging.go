// Package logging holds the -debug switch that goclusterfs-mount,
// goclusterfs-fsck and gfsctl share.
package logging

import (
	"fmt"
	"log"
	"sync/atomic"
)

var debug atomic.Bool

// SetDebug turns Debugf output on or off.
func SetDebug(on bool) { debug.Store(on) }

// Debugf logs like log.Printf with a [DEBUG] prefix, and only after SetDebug(true).
func Debugf(format string, args ...any) {
	if debug.Load() {
		_ = log.Output(2, "[DEBUG] "+fmt.Sprintf(format, args...))
	}
}

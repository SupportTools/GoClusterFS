package logging

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestDebugfPrintsOnlyWhenOn(t *testing.T) {
	var out bytes.Buffer
	log.SetOutput(&out)
	defer log.SetOutput(log.Writer())
	defer SetDebug(false)

	Debugf("hidden %d", 1)
	SetDebug(true)
	Debugf("shown %d", 2)

	if got := out.String(); strings.Contains(got, "hidden") || !strings.Contains(got, "[DEBUG] shown 2") {
		t.Errorf("log output = %q", got)
	}
}

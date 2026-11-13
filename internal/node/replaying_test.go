package node

import (
	"errors"
	"reflect"
	"testing"
)

func TestReplayingListsSlotsWhileTheirHandlerRuns(t *testing.T) {
	var r Registry
	var during []int
	handler := func(*FailedNodeInfo) error {
		during = r.Replaying()
		return errors.New("replay failed")
	}
	r.replaying.Or(1 << 63)
	if err := r.replay(handler, 3, &FailedNodeInfo{}); err == nil {
		t.Fatal("replay hid the handler's error")
	}
	if want := []int{3, 63}; !reflect.DeepEqual(during, want) {
		t.Errorf("Replaying during the handler = %v, want %v", during, want)
	}
	if got := r.Replaying(); !reflect.DeepEqual(got, []int{63}) {
		t.Errorf("Replaying after the handler = %v, want [63]", got)
	}
}

package hsm_test

import (
	"testing"

	"github.intel.com/hpdd/lustre/hsm"
)

// Basic test to ensure that the test API implements all interfaces
func TestActionSource(t *testing.T) {
	src := hsm.TestActionSource()
	defer src.Stop()

	next := <-src.Actions()
	handle, err := next.Begin(0, false)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if handle.Action() != next.Action() {
		t.Fatalf("err: huh?")
	}
}

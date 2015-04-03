package changelog

import (
	"fmt"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/llapi"
)

// CreateHandle returns a Handle for accessing Changelog records
// on a given MDT.
func CreateHandle(device string) lustre.ChangelogHandle {
	return &changelogHandle{
		device: device,
	}
}

// Clear is a convenience function to enable clearing a changelog
// without first creating a Handle.
func Clear(device, token string, endRec int64) error {
	return CreateHandle(device).Clear(token, endRec)
}

type changelogHandle struct {
	open   bool
	device string
	cl     *llapi.Changelog
}

// Open sets up the Changelog for reading from the first available record
func (h *changelogHandle) Open(follow bool) error {
	return h.OpenAt(1, follow)
}

// OpenAt sets up the Changelog for reading from the specified record index
func (h *changelogHandle) OpenAt(startRec int64, follow bool) error {
	var err error

	if h.open {
		return nil
	}

	h.cl, err = llapi.ChangelogStart(h.device, startRec, follow)
	if err != nil {
		h.cl = nil
		return err
	}

	h.open = true
	return nil
}

// Close closes the Changelog handle
func (h *changelogHandle) Close() error {
	h.open = false
	return llapi.ChangelogFini(h.cl)
}

// NextRecord retrieves the next available record
func (h *changelogHandle) NextRecord() (lustre.ChangelogRecord, error) {
	if !h.open {
		return nil, fmt.Errorf("NextRecord() called on closed handle")
	}
	return llapi.ChangelogRecv(h.cl)
}

// Clear clears Changelog records for the specified token up to the supplied
// end record index
func (h *changelogHandle) Clear(token string, endRec int64) error {
	return llapi.ChangelogClear(h.device, token, endRec)
}

func (h *changelogHandle) String() string {
	return h.device
}

/*
 * FIXME: This implementation is racy. When run with the race detector
 * enabled, it flags racy access to the handle.open bool. Plus, it doesn't
 * return all records as expected. It used to work before everything got
 * shuffled around, so leaving this here as a TODO.

// Follower is a Lustre Changelog follower. It provides a work-around
// for the broken CHANGELOG_FLAG_FOLLOW functionality in liblustreapi.
// If that is ever fixed, then it should be possible to just call
// NextRecord() in a blocking loop directly on the handle.
type Follower struct {
	handle    lustre.ChangelogHandle
	records   chan lustre.ChangelogRecord
	err       chan error
	nextIndex int64
}

// Close calls Close() on the wrapped Handle.
func (f *Follower) Close() error {
	return f.handle.Close()
}

// Follow opens the wrapped Handle at the first available index.
func (f *Follower) Follow() {
	f.FollowFrom(1)
}

// FollowFrom opens the wrapped Handle at the specified index.
func (f *Follower) FollowFrom(startRec int64) {
	f.nextIndex = startRec
	records := make(chan lustre.ChangelogRecord, 0)
	f.records = records

	go func(h lustre.ChangelogHandle) {
		for {
			if err := h.OpenAt(f.nextIndex, false); err != nil {
				f.err <- err
				return
			}

			r, err := h.NextRecord()
			for err == nil {
				records <- r
				f.nextIndex = r.Index() + 1
				r, err = h.NextRecord()
			}
			if err != io.EOF {
				f.err <- err
				h.Close()
				return
			}
			h.Close()

			time.Sleep(1 * time.Second)
		}
	}(f.handle)
}

// NextRecord blocks until the next record is available or an error was
// encountered by the follower goroutine.
func (f *Follower) NextRecord() (lustre.ChangelogRecord, error) {
	select {
	case r := <-f.records:
		return r, nil
	case err := <-f.err:
		return nil, err
	}
}

// FollowHandle takes a Handle and wraps it with a Follower object.
func FollowHandle(h lustre.ChangelogHandle, startRec int64) *Follower {
	f := &Follower{
		handle: h,
	}

	f.FollowFrom(startRec)

	return f
}

// CreateFollower takes a MDT name and returns a Follower-wrapped Handle
func CreateFollower(device string, startRec int64) *Follower {
	h := CreateHandle(device)
	return FollowHandle(h, startRec)
}
*/

package follower

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.intel.com/hpdd/lustre/changelog"
)

// Follower is a Lustre Changelog follower. It provides a work-around
// for the broken CHANGELOG_FLAG_FOLLOW functionality in liblustreapi.
// If that is ever fixed, then it should be possible to just call
// NextRecord() in a blocking loop directly on the handle.
type Follower struct {
	handle    changelog.Handle
	records   chan changelog.Record
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
	records := make(chan changelog.Record, 0)
	f.records = records

	go func() {
		for {
			if err := f.handle.OpenAt(f.nextIndex, false); err != nil {
				fmt.Fprintf(os.Stderr, "Error opening changelog (%s): %s\n", f.handle, err)
				return
			}

			r, err := f.handle.NextRecord()
			for err == nil {
				records <- r
				f.nextIndex = r.Index() + 1
				r, err = f.handle.NextRecord()
			}
			if err != io.EOF {
				f.err <- err
				f.handle.Close()
				return
			}
			f.handle.Close()

			time.Sleep(1 * time.Second)
		}
	}()
}

// NextRecord blocks until the next record is available or an error was
// encountered by the follower goroutine.
func (f *Follower) NextRecord() (changelog.Record, error) {
	select {
	case r := <-f.records:
		return r, nil
	case err := <-f.err:
		return nil, err
	}
}

// Create takes a Handle and wraps it with a Follower object.
func Create(h changelog.Handle, startRec int64) *Follower {
	f := &Follower{
		handle: h,
	}

	f.FollowFrom(startRec)

	return f
}

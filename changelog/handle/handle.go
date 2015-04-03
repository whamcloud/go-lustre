package handle

import (
	"fmt"

	"github.intel.com/hpdd/lustre/changelog"
	"github.intel.com/hpdd/lustre/llapi"
)

// Create returns a Handle for accessing Changelog records
// on a given MDT.
func Create(device string) changelog.Handle {
	return &changelogHandle{
		device: device,
	}
}

// Clear is a convenience function to enable clearing a changelog
// without first creating a Handle.
func Clear(device, token string, endRec int64) error {
	return Create(device).Clear(token, endRec)
}

// ChangelogHandle represents a Lustre Changelog
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
func (h *changelogHandle) NextRecord() (changelog.Record, error) {
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

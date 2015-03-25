package handle

import (
	"github.intel.com/hpdd/lustre/changelog"
	"github.intel.com/hpdd/lustre/llapi"
)

// Create returns a Handle for accessing Changelog records
// on a given MDT.
func Create(device string) changelog.Handle {
	return llapi.CreateChangelogHandle(device)
}

// Clear is a convenience function to enable clearing a changelog
// without first creating a Handle.
func Clear(device, token string, endRec int64) error {
	return Create(device).Clear(token, endRec)
}

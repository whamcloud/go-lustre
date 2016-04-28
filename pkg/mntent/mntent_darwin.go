package mntent

import "github.intel.com/hpdd/lustre"

// GetMounted returns a slide of filesystem entries from
// the mounted fs table.
func GetMounted() (Entries, error) {
	return nil, lustre.ErrUnimplemented
}

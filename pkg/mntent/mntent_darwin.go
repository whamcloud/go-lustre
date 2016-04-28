package mntent

import "errors"

// GetMounted returns a slide of filesystem entries from
// the mounted fs table.
func GetMounted() (Entries, error) {
	return nil, errors.New("not implemented")
}

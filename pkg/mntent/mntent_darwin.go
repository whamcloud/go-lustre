package mntent

import "errors"

// GetMounted returns a slide of filesystem entries from
// the mounted fs table.
func GetMounted() ([]*Entry, error) {
	return nil, errors.New("not implemented")
}

// GetEntryByDir returns the mounted filesystem entry for
// the provided mount point.
func GetEntryByDir(dir string) (*Entry, error) {
	return nil, errors.New("not implemented")
}

// GetEntriesByType returns a slice of mounted filesystem
// entries that match the provided type.
func GetEntriesByType(fstype string) ([]*Entry, error) {
	return nil, errors.New("not implemented")
}

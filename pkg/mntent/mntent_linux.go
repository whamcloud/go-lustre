package mntent

import "os"

// GetMounted returns a slide of filesystem entries from
// the mounted fs table.
func GetMounted() (Entries, error) {
	fp, err := os.Open("/etc/mtab")
	if err != nil {
		return nil, err
	}
	return getEntries(fp)
}

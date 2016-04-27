package mntent

import "fmt"

// Entry is an entry in a filesystem table.
type Entry struct {
	Fsname string
	Dir    string
	Type   string
	Opts   string
	Freq   int
	Passno int
}

func (e *Entry) String() string {
	return fmt.Sprintf("%s %s %s %s %d %d", e.Fsname, e.Dir, e.Type, e.Opts, e.Freq, e.Passno)
}

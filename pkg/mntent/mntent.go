package mntent

// Entry is an entry in a filesystem table.
type Entry struct {
	Fsname string
	Dir    string
	Type   string
	Opts   string
	Freq   int
	Passno int
}

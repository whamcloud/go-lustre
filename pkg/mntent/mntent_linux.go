package mntent

// Go wrapper around getmntent_r

//
// #include <stdio.h>
// #include <stdlib.h>
// #include <paths.h>
// #include <mntent.h>
//
import "C"
import (
	"errors"
	"path/filepath"
	"unsafe"
)

func getEntries(fp *C.FILE) ([]*Entry, error) {
	bufLen := 4096
	buffer := C.malloc(C.size_t(bufLen))
	defer C.free(buffer)

	var mntent C.struct_mntent
	var entries []*Entry
	for {
		ret, err := C.getmntent_r(fp, &mntent, (*C.char)(buffer), C.int(bufLen))
		if err != nil {
			return nil, err
		}
		if ret == nil {
			break
		}
		entries = append(entries, &Entry{
			Fsname: C.GoString(mntent.mnt_fsname),
			Dir:    C.GoString(mntent.mnt_dir),
			Type:   C.GoString(mntent.mnt_type),
			Opts:   C.GoString(mntent.mnt_opts),
			Freq:   int(mntent.mnt_freq),
			Passno: int(mntent.mnt_passno),
		})
	}
	return entries, nil
}

// GetMounted returns a slide of filesystem entries from
// the mounted fs table.
func GetMounted() ([]*Entry, error) {
	path := C.CString("/etc/mtab")
	defer C.free(unsafe.Pointer(path))

	mode := C.CString("r")
	defer C.free(unsafe.Pointer(mode))

	fp, err := C.setmntent(path, mode)
	if err != nil {
		return nil, err
	}
	return getEntries(fp)
}

// GetEntryByDir returns the mounted filesystem entry for
// the provided mount point.
func GetEntryByDir(dir string) (*Entry, error) {
	dir = filepath.Clean(dir)
	entries, err := GetMounted()
	if err != nil {
		return nil, err
	}

	for _, mnt := range entries {
		if mnt.Dir == dir {
			return mnt, nil
		}
	}
	return nil, errors.New("Mount point not found")
}

// GetEntriesByType returns a slice of mounted filesystem
// entries that match the provided type.
func GetEntriesByType(fstype string) ([]*Entry, error) {
	entries, err := GetMounted()
	if err != nil {
		return nil, err
	}
	var selected []*Entry
	for _, mnt := range entries {
		if mnt.Type == fstype {
			selected = append(selected, mnt)
		}
	}
	return selected, nil
}

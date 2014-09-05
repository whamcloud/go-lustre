package lustre

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
//
import "C"

import (
	"fmt"
	"os"
	"path"
)

var (
	// Special FID for .lustre
	_DOT_LUSTRE_FID = Fid{0x200000002, 0x1, 0x0}
)

type Fid C.lustre_fid

func (fid Fid) String() string {
	return fmt.Sprintf("[0x%x:0x%x:0x%x]",
		fid.f_seq, fid.f_oid, fid.f_ver)
}

func (fid Fid) IsZero() bool {
	return fid.f_seq == 0 && fid.f_oid == 0 && fid.f_ver == 0
}

func LookupFid(path string) (*Fid, error) {
	fid := Fid{}
	_, err := C.llapi_path2fid(C.CString(path), (*C.lustre_fid)(&fid))
	if err != nil {
		fmt.Println(path, err)
		return nil, err
	}
	return &fid, nil
}

// ParseFid converts a fid in string format to a Fid
func ParseFid(fidstr string) (*Fid, error) {
	fid := Fid{}
	n, err := fmt.Sscanf(fidstr, "[0x%x:0x%x:0x%x]", &fid.f_seq, &fid.f_oid, &fid.f_ver)
	if err != nil {
		return nil, err
	}
	if n != 3 {
		return nil, fmt.Errorf("lustre: unable to parse fid string: %v", fidstr)
	}
	return &fid, nil
}

// Pathnames returns all paths for a FID.
//
// This returns a slice containing all names that reference
// the FID.
//
func (fid Fid) Pathnames(mnt RootDir) ([]string, error) {
	return FidPathnames(mnt, fid.String())
}

// Path returns the fid path.
func (fid Fid) Path(mnt RootDir) string {
	return FidPath(mnt, fid.String())
}

// Open by fid.
// Returns readable file handle
func (fid Fid) Open(mnt RootDir) (*os.File, error) {
	return os.Open(fid.Path(mnt))
}

// Open by fid.
// Returns readable file handle
func (fid Fid) OpenFile(mnt RootDir, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(fid.Path(mnt), flags, perm)
}

// Pathname returns a path for a FID.
//
// If the fid is referred to by more than one file (i.e. hard links),
// the the LINKNO specifies a specific link to return. This does
// not update linkno on return. Use Paths to retrieve all hard link
// names.
//
func FidPathname(mnt RootDir, fidstr string, linkno int) (string, error) {
	var recno int64 = 0
	return fid2path(string(mnt), fidstr, &recno, &linkno)
}

// FidPath returns the Fid Path for a fid.
func FidPath(mnt RootDir, fidstr string) string {
	return path.Join(string(mnt), ".lustre", "fid", fidstr)
}

// Pathnames returns all paths for a FIDSTR.
//
// This returns a slice containing all names that reference
// the FID.
//
func FidPathnames(mnt RootDir, fidstr string) ([]string, error) {
	var recno int64 = 0
	var linkno int = 0
	var prev_linkno int = -1
	var paths = make([]string, 0)
	for prev_linkno < linkno {
		prev_linkno = linkno
		p, err := fid2path(string(mnt), fidstr, &recno, &linkno)
		if err != nil {
			return paths, err
		}
		paths = append(paths, path.Join(string(mnt), p))
	}

	return paths, nil

}

type FidPathError struct {
	Fid string
	Rc  int
	Err error
}

func (e *FidPathError) Error() string {
	return fmt.Sprintf("fid2path: %s failed: %d %v", e.Fid, e.Rc, e.Err)
}

func fid2path(device string, fidstr string, recno *int64, linkno *int) (string, error) {
	var buffer [4096]C.char
	var clinkno C.int = C.int(*linkno)
	rc, err := C.llapi_fid2path(C.CString(device), C.CString(fidstr),
		&buffer[0],
		C.int(len(buffer)),
		(*C.longlong)(recno),
		&clinkno)
	*linkno = int(clinkno)
	if rc != 0 || err != nil {
		return "", &FidPathError{fidstr, int(rc), err}
	}
	p := C.GoString(&buffer[0])
	return p, err
}

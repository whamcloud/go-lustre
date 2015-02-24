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

// Fid is a Lustre file identifier.
type Fid interface {
	String() string
	IsZero() bool
	IsDotLustre() bool
	Pathnames(RootDir) ([]string, error)
	AbsPathnames(RootDir) ([]string, error)
	Path(RootDir) string
	Open(RootDir) (*os.File, error)
	OpenFile(RootDir, int, os.FileMode) (*os.File, error)
	Stat(RootDir) (os.FileInfo, error)
	Lstat(RootDir) (os.FileInfo, error)
	MarshalJSON() ([]byte, error)
	UnMarshalJSON([]byte) error
}

type fid struct {
	cfid *C.lustre_fid
}

// NewFid takes a *C.lustre_fid and returns a Go wrapper for it.
func NewFid(cfid *C.lustre_fid) Fid {
	return &fid{
		cfid: cfid,
	}
}

func (f *fid) String() string {
	return fmt.Sprintf("[0x%x:0x%x:0x%x]", f.cfid.f_seq, f.cfid.f_oid, f.cfid.f_ver)
}

// IsZero is true if Fid is 0.
func (f *fid) IsZero() bool {
	return f.cfid.f_seq == 0 && f.cfid.f_oid == 0 && f.cfid.f_ver == 0
}

// IsDotLustre is true if Fid is special .lustre entry.
func (f *fid) IsDotLustre() bool {
	return f.cfid.f_seq == 0x200000002 && f.cfid.f_oid == 0x1 && f.cfid.f_ver == 0x0
}

// Pathnames returns all paths for a FID.
//
// This returns a slice containing all names that reference
// the FID.
//
func (f *fid) Pathnames(mnt RootDir) ([]string, error) {
	return FidPathnames(mnt, f.String())
}

// AbsPathnames returns all paths for a FID.
//
// This returns a slice containing all names that reference
// the FID.
//
func (f *fid) AbsPathnames(mnt RootDir) ([]string, error) {
	return FidAbsPathnames(mnt, f.String())
}

// Path returns the "open by fid" path.
func (f *fid) Path(mnt RootDir) string {
	return FidPath(mnt, f.String())
}

// Open by fid.
// Returns readable file handle
func (f *fid) Open(mnt RootDir) (*os.File, error) {
	return os.Open(f.Path(mnt))
}

// OpenFile by fid.
// Returns readable file handle
func (f *fid) OpenFile(mnt RootDir, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(f.Path(mnt), flags, perm)
}

// Stat by fid.
// Returns readable file handle
func (f *fid) Stat(mnt RootDir) (os.FileInfo, error) {
	return os.Stat(f.Path(mnt))
}

// Lstat by fid.
// Returns readable file handle
func (f *fid) Lstat(mnt RootDir) (os.FileInfo, error) {
	return os.Lstat(f.Path(mnt))
}

// MarshalJSON converts a Fid to a string for JSON.
func (f *fid) MarshalJSON() ([]byte, error) {
	return []byte(`"` + f.String() + `"`), nil
}

// UnMarshalJSON converts fid string to Fid.
func (f *fid) UnMarshalJSON(b []byte) (err error) {
	newFid, err := ParseFid(string(b))
	f = newFid.(*fid)
	return err
}

// LookupFid returns the Fid for the given file or an error.
func LookupFid(path string) (Fid, error) {
	cfid := C.lustre_fid{}
	rc, err := C.llapi_path2fid(C.CString(path), (*C.lustre_fid)(&cfid))
	if rc < 0 {
		return &fid{}, fmt.Errorf("%s: fid not found (%s)", path, err.Error())
	}
	return NewFid(&cfid), nil
}

// ParseFid converts a fid in string format to a Fid
func ParseFid(fidstr string) (Fid, error) {
	cfid := C.lustre_fid{}
	if fidstr[0] == '[' {
		fidstr = fidstr[1 : len(fidstr)-1]
	}
	n, err := fmt.Sscanf(fidstr, "0x%x:0x%x:0x%x", &cfid.f_seq, &cfid.f_oid, &cfid.f_ver)
	if err != nil || n != 3 {
		return &fid{}, fmt.Errorf("lustre: unable to parse fid string: %v", fidstr)
	}
	return NewFid(&cfid), nil
}

// FidPathname returns a path for a FID.
//
// Paths are relative from the RootDir of the filesystem.
// If the fid is referred to by more than one file (i.e. hard links),
// the the LINKNO specifies a specific link to return. This does
// not update linkno on return. Use Paths to retrieve all hard link
// names.
//
func FidPathname(mnt RootDir, fidstr string, linkno int) (string, error) {
	var recno int64
	return fid2path(string(mnt), fidstr, &recno, &linkno)
}

// FidPath returns the Fid Path for a fid.
func FidPath(mnt RootDir, fidstr string) string {
	return path.Join(string(mnt), ".lustre", "fid", fidstr)
}

func fidPathnames(mnt RootDir, fidstr string, absPath bool) ([]string, error) {
	var recno int64
	var linkno int
	var prevLinkno = -1
	var paths = make([]string, 0)
	for prevLinkno < linkno {
		prevLinkno = linkno
		p, err := fid2path(string(mnt), fidstr, &recno, &linkno)
		if err != nil {
			return paths, err
		}

		if absPath {
			p = path.Join(string(mnt), p)
		}
		paths = append(paths, p)

	}
	return paths, nil
}

// FidAbsPathnames returns all paths for a FIDSTR.
//
// This returns a slice containing all names that reference
// the FID.
//
func FidAbsPathnames(mnt RootDir, fidstr string) ([]string, error) {
	return fidPathnames(mnt, fidstr, true)
}

// FidPathnames returns all paths for a FIDSTR.
//
// This returns a slice containing all names that reference
// the FID.
//
func FidPathnames(mnt RootDir, fidstr string) ([]string, error) {
	return fidPathnames(mnt, fidstr, false)
}

// FidPathError is an error that occurs while retrieving the pathname for a fid.
//
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
	var clinkno = C.int(*linkno)
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

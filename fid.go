package lustre

import (
	"fmt"
	"os"
	"path"

	"github.intel.com/hpdd/lustre/llapi"
)

// Fid is a Lustre file identifier.
type Fid interface {
	String() string
	IsZero() bool
	IsDotLustre() bool
	Path(RootDir) string
	Pathnames(RootDir) ([]string, error)
	AbsPathnames(RootDir) ([]string, error)
	Open(RootDir) (*os.File, error)
	OpenFile(RootDir, int, os.FileMode) (*os.File, error)
	Stat(RootDir) (os.FileInfo, error)
	Lstat(RootDir) (os.FileInfo, error)
	MarshalJSON() ([]byte, error)
	UnMarshalJSON([]byte) error
}

type fid struct {
	cfid *llapi.CFid
}

// NewFid takes a *llapi.CFid and returns a Go wrapper for it.
func NewFid(cfid *llapi.CFid) Fid {
	return &fid{
		cfid: cfid,
	}
}

func (f *fid) String() string {
	return f.cfid.String()
}

// IsZero is true if Fid is 0.
func (f *fid) IsZero() bool {
	return f.cfid.IsZero()
}

// IsDotLustre is true if Fid is special .lustre entry.
func (f *fid) IsDotLustre() bool {
	return f.cfid.IsDotLustre()
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
	cfid, err := llapi.Path2Fid(path)
	if err != nil {
		return nil, fmt.Errorf("%s: fid not found (%s)", path, err.Error())
	}
	return NewFid(cfid), nil
}

// ParseFid converts a fid in string format to a Fid
func ParseFid(fidstr string) (Fid, error) {
	cfid, err := llapi.ParseFid(fidstr)
	if err != nil {
		return nil, err
	}
	return NewFid(cfid), nil
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
	return llapi.Fid2Path(string(mnt), fidstr, &recno, &linkno)
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
		p, err := llapi.Fid2Path(string(mnt), fidstr, &recno, &linkno)
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

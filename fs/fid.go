package fs

import (
	"fmt"
	"os"
	"path"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/lustre/luser"
)

// LookupFid returns the Fid for the given file or an error.
func LookupFid(path string) (*lustre.Fid, error) {
	fid, err := luser.GetFid(path)
	if err != nil {
		// XXX Be noisy for testing, but this fallback shouldn't be required
		//glog.Errorf("%v: %v", path, err)
		fid, err = llapi.Path2Fid(path)
		if err != nil {
			return nil, fmt.Errorf("%s: fid not found (%s)", path, err.Error())
		}
	}
	return fid, nil
}

/*
Slow version...
func LookupFid(path string) (*lustre.Fid, error) {
	fid, err := llapi.Path2Fid(path)
	if err != nil {
		return nil, fmt.Errorf("%s: fid not found (%s)", path, err.Error())
	}
	return fid, nil
}
*/

// FidPath returns the open-by-fid path for a fid.
func FidPath(mnt RootDir, f *lustre.Fid) string {
	return path.Join(string(mnt), FidRelativePath(f))
}

// FidRelativePath returns the relattive open-by-fid path for a fid.
func FidRelativePath(f *lustre.Fid) string {
	return path.Join(".lustre", "fid", f.String())
}

// FidPathname returns a path for a FID.
//
// Paths are relative from the RootDir of the filesystem.
// If the fid is referred to by more than one file (i.e. hard links),
// the the LINKNO specifies a specific link to return. This does
// not update linkno on return. Use Paths to retrieve all hard link
// names.
//
func FidPathname(mnt RootDir, f *lustre.Fid, linkno int) (string, error) {
	var recno int64
	return llapi.Fid2Path(string(mnt), f, &recno, &linkno)
}

// FidPathnames returns all paths for a fid.
//
// This returns a slice containing all names that reference
// the fid.
//
func FidPathnames(mnt RootDir, f *lustre.Fid) ([]string, error) {
	return fidPathnames(mnt, f, false)
}

func fidPathnames(mnt RootDir, f *lustre.Fid, absPath bool) ([]string, error) {
	var recno int64
	var linkno int
	var prevLinkno = -1
	var paths = make([]string, 0)
	for prevLinkno < linkno {
		prevLinkno = linkno
		p, err := llapi.Fid2Path(string(mnt), f, &recno, &linkno)
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

// StatFid returns an os.FileInfo given a mountpoint and fid
func StatFid(mnt RootDir, f *lustre.Fid) (os.FileInfo, error) {
	return os.Stat(FidPath(mnt, f))
}

// LstatFid returns an os.FileInfo given a mountpoint and fid
func LstatFid(mnt RootDir, f *lustre.Fid) (os.FileInfo, error) {
	return os.Lstat(FidPath(mnt, f))
}

// OpenByFid returns an open file handle given a mountpoint and fid
func OpenByFid(mnt RootDir, f *lustre.Fid) (*os.File, error) {
	return os.Open(FidPath(mnt, f))
}

// OpenFileByFid returns an open file handle given a mountpoint and fid
func OpenFileByFid(mnt RootDir, f *lustre.Fid, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(FidPath(mnt, f), flags, perm)
}

/*
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

// ParseFid converts a fid in string format to a Fid
func ParseFid(fidstr string) (Fid, error) {
	cfid, err := llapi.ParseFid(fidstr)
	if err != nil {
		return nil, err
	}
	return NewFid(cfid), nil
}

// FidAbsPathnames returns all paths for a FIDSTR.
//
// This returns a slice containing all names that reference
// the FID.
//
func FidAbsPathnames(mnt RootDir, fidstr string) ([]string, error) {
	return fidPathnames(mnt, fidstr, true)
}

*/

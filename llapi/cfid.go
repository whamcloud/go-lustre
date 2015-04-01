package llapi

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
//
import "C"
import (
	"fmt"
	"path"
)

type CFid C.lustre_fid

// Path2Fid the CFid for the given file or an error.
func Path2Fid(path string) (*CFid, error) {
	cfid := C.lustre_fid{}
	rc, err := C.llapi_path2fid(C.CString(path), (*C.lustre_fid)(&cfid))
	if err := isError(rc, err); err != nil {
		return nil, fmt.Errorf("%s: fid not found (%s)", path, err.Error())
	}
	return (*CFid)(&cfid), nil
}
func (f *CFid) String() string {
	return fmt.Sprintf("[0x%x:0x%x:0x%x]", f.f_seq, f.f_oid, f.f_ver)
}

// IsZero is true if Fid is 0.
func (f *CFid) IsZero() bool {
	return f.f_seq == 0 && f.f_oid == 0 && f.f_ver == 0
}

// IsDotLustre is true if Fid is special .lustre entry.
func (f *CFid) IsDotLustre() bool {
	return f.f_seq == 0x200000002 && f.f_oid == 0x1 && f.f_ver == 0x0
}

// FidPath returns the Fid Path for a fid.
// Path returns the "open by fid" path.
func (f *CFid) Path(mnt string) string {
	return fidPath(mnt, f.String())
}

func fidPath(mnt string, fidstr string) string {
	return path.Join(string(mnt), ".lustre", "fid", fidstr)
}

// MarshalJSON converts a CFid to a string for JSON.
func (f *CFid) MarshalJSON() ([]byte, error) {
	return []byte(`"` + f.String() + `"`), nil
}

// UnMarshalJSON converts fid string to CFid.
func (f *CFid) UnMarshalJSON(b []byte) (err error) {
	newFid, err := ParseFid(string(b))
	f = newFid
	return err
}

// ParseFid converts a fid in string format to a Fid
func ParseFid(fidstr string) (*CFid, error) {
	cfid := C.lustre_fid{}
	if fidstr[0] == '[' {
		fidstr = fidstr[1 : len(fidstr)-1]
	}
	n, err := fmt.Sscanf(fidstr, "0x%x:0x%x:0x%x", &cfid.f_seq, &cfid.f_oid, &cfid.f_ver)
	if err != nil || n != 3 {
		return nil, fmt.Errorf("lustre: unable to parse fid string: %v", fidstr)
	}
	return (*CFid)(&cfid), nil
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

// Fid2Path returns next path for given fid.
func Fid2Path(device string, fidstr string, recno *int64, linkno *int) (string, error) {
	var buffer [4096]C.char
	var clinkno = C.int(*linkno)
	rc, err := C.llapi_fid2path(C.CString(device), C.CString(fidstr),
		&buffer[0],
		C.int(len(buffer)),
		(*C.longlong)(recno),
		&clinkno)
	*linkno = int(clinkno)
	if err := isError(rc, err); err != nil {
		return "", &FidPathError{fidstr, int(rc), err}
	}
	p := C.GoString(&buffer[0])
	return p, err
}

func GetMdtIndexbyFid(mountFd int, f *CFid) (int, error) {
	var mdtIndex C.int

	rc, err := C.llapi_get_mdt_index_by_fid(C.int(mountFd), (*C.lustre_fid)(f), &mdtIndex)
	if err := isError(rc, err); err != nil {
		return 0, err
	}

	return int(mdtIndex), nil
}

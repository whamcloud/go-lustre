// Package lustre provides access to many of the functions avialable in liblustreapi.
//
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

// Version returns the current Lustre version string.
func Version() string {
	var buffer [4096]C.char
	var cversion *C.char
	var version string
	_, err := C.llapi_get_version(&buffer[0], C.int(len(buffer)),
		&cversion)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	version = C.GoString(cversion)
	return version
}

func MountId(mountPath string) (string, error) {
	var buffer [2048]C.char
	rc, err := C.llapi_getname(C.CString(mountPath), &buffer[0], C.size_t(len(buffer)))
	if rc < 0 || err != nil {
		return "", fmt.Errorf("lustre:  %v %d %v", mountPath, rc, err)
	}
	return C.GoString(&buffer[0]), nil
}

type RootDir string

func isDotLustre(dir string) bool {
	fi, err := os.Lstat(dir)
	if err != nil {
		return false
	}
	if fi.Mode().IsDir() {
		fid, err := LookupFid(dir)
		if err == nil && *fid == _DOT_LUSTRE_FID {
			return true
		}
	}
	return false
}

func findRoot(pathname string) string {
	if pathname == "" {
		return ""
	}
	if isDotLustre(path.Join(pathname, ".lustre")) {
		return pathname
	}
	if pathname == "/" {
		return ""
	}
	return findRoot(path.Dir(pathname))

}

// MountRoot returns the Lustre filesystem mountpoint for the give path
// or returns an error if the path is not on a Lustre filesystem.
func MountRoot(path string) (RootDir, error) {
	mnt := findRoot(path)
	if mnt == "" {
		return RootDir(""), fmt.Errorf("%s not a Lustre filesystem", path)
	}
	return RootDir(mnt), nil
}

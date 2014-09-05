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
	"syscall"
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

// MountId returns the local Lustre client indentifier for that mountpoint. This can
// be used to determine which entries in /proc/fs/lustre as associated with
// that client.
func MountId(mountPath string) (string, error) {
	var buffer [2048]C.char
	rc, err := C.llapi_getname(C.CString(mountPath), &buffer[0], C.size_t(len(buffer)))
	if rc < 0 || err != nil {
		return "", fmt.Errorf("lustre:  %v %d %v", mountPath, rc, err)
	}
	return C.GoString(&buffer[0]), nil
}

type RootDir string

// Determine if given directory is the one true magical DOT_LUSTRE directory.
func isDotLustre(dir string) bool {
	fi, err := os.Lstat(dir)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		fid, err := LookupFid(dir)
		if err == nil && *fid == _DOT_LUSTRE_FID {
			return true
		}
	}
	return false
}

// Return root device from the struct stat embedded in FileInfo
func rootDevice(fi os.FileInfo) uint64 {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if ok {
		return stat.Dev
	}
	panic("no stat available")
}

// findRoot returns the root directory for the lustre filesystem containing
// the pathname. If the the filesystem is not lustre, then error is returned.
func findRoot(dev uint64, pathname string) string {
	parent := path.Dir(pathname)
	fi, err := os.Lstat(parent)
	if err != nil {
		return ""
	}
	//  If "/" is lustre then we won't see the device change
	if rootDevice(fi) != dev || pathname == "/" {
		if isDotLustre(path.Join(pathname, ".lustre")) {
			return pathname
		} else {
			return ""
		}
	}

	return findRoot(dev, parent)
}

// MountRoot returns the Lustre filesystem mountpoint for the give path
// or returns an error if the path is not on a Lustre filesystem.
func MountRoot(path string) (RootDir, error) {
	fi, err := os.Lstat(path)
	if err != nil {
		return RootDir(""), err
	}

	mnt := findRoot(rootDevice(fi), path)
	if mnt == "" {
		return RootDir(""), fmt.Errorf("%s not a Lustre filesystem", path)
	}
	return RootDir(mnt), nil
}

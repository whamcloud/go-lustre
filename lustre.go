// Package lustre provides access to many of the functions avialable in liblustreapi.
//
// Currently, this includes the HSM Copytool API, managing Fids, and reading changelogs.
package lustre

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
//
import "C"

import (
	"fmt"
	"hpdd/lustre/status"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
)

const (
	PROC_DIR = "/proc/fs/lustre"
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
func MountId(mountPath string) (*status.LustreClient, error) {
	var buffer [2048]C.char
	rc, err := C.llapi_getname(C.CString(mountPath), &buffer[0], C.size_t(len(buffer)))
	if rc < 0 || err != nil {
		return nil, fmt.Errorf("lustre:  %v %d %v", mountPath, rc, err)
	}
	id := C.GoString(&buffer[0])
	elem := strings.Split(id, "-")
	c := status.LustreClient{elem[0], elem[1]}
	return &c, nil
}

// Given a mountpoint, return the filesystem's name.
func FilesystemName(mountPath string) (string, error) {
	var buffer [2048]C.char
	rc, err := C.llapi_search_fsname(C.CString(mountPath), &buffer[0])
	if rc < 0 || err != nil {
		return "", fmt.Errorf("lustre:  %v %d %v", mountPath, rc, err)
	}
	return C.GoString(&buffer[0]), nil
}

// Given a filesystem name, find the local mountpoint.
func FilesystemName2Mount(fsName string) (RootDir, error) {
	var buffer [C.PATH_MAX]C.char
	rc, err := C.llapi_search_rootpath(&buffer[0], C.CString(fsName))
	if rc < 0 || err != nil {
		return RootDir(""), fmt.Errorf("lustre:  %v %d %v", fsName, rc, err)
	}
	return RootDir(C.GoString(&buffer[0])), nil
}

// Get the filesystem's ID. For the moment, this is the FS name, but in
// the future it could be something more globally unique (uuid?).
func FilesystemId(mountPath string) (string, error) {
	id, err := FilesystemName(mountPath)
	if err != nil {
		return "", err
	}
	return id, nil
}

// Given a filesystem id (as returned by FilesystemId), find the local
// mountpoint.
func FilesystemId2Mount(fsId string) (RootDir, error) {
	mnt, err := FilesystemName2Mount(fsId)
	if err != nil {
		return mnt, err
	}
	return mnt, nil
}

type RootDir string

// Join args with root dir to create an absolute path.
// FIXME: replace this with OpenAt and friends
func (root RootDir) Join(args ...string) string {
	return path.Join(string(root), path.Join(args...))
}

// Just converts it into a regular string.
func (root RootDir) String() string {
	return string(root)
}

// Determine if given directory is the one true magical DOT_LUSTRE directory.
func isDotLustre(dir string) bool {
	fi, err := os.Lstat(dir)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		fid, err := LookupFid(dir)
		if err == nil && fid == _DOT_LUSTRE_FID {
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

// findRelPah returns pathname relative to root directory for the lustre filesystem containing
// the pathname. If no Lustre root was found, then empty strings are returned.
func findRelPath(dev uint64, pathname string, relPath []string) (string, string) {
	parent := path.Dir(pathname)
	fi, err := os.Lstat(parent)
	if err != nil {
		return "", ""
	}
	//  If "/" is lustre then we won't see the device change
	if rootDevice(fi) != dev || pathname == "/" {
		if isDotLustre(path.Join(pathname, ".lustre")) {
			return pathname, path.Join(relPath...)
		} else {
			return "", ""
		}
	}

	return findRelPath(dev, parent, append([]string{path.Base(pathname)}, relPath...))
}

// MountRelPath returns the lustre mountpoint, and remaing path for the given pathname. The remaining  paht
// is relative to the mount point. Returns an error if pathname is not valid or does not refer to a Lustre fs.
func MountRelPath(pathname string) (RootDir, string, error) {
	pathname = filepath.Clean(pathname)
	fi, err := os.Lstat(pathname)
	if err != nil {
		return RootDir(""), "", err
	}

	root, relPath := findRelPath(rootDevice(fi), pathname, []string{})
	if root == "" {
		return RootDir(""), "", fmt.Errorf("%s not a Lustre filesystem", pathname)
	}
	return RootDir(root), relPath, nil
}

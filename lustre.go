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
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.intel.com/hpdd/lustre/status"
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

// MountID returns the local Lustre client indentifier for that mountpoint. This can
// be used to determine which entries in /proc/fs/lustre as associated with
// that client.
func MountID(mountPath string) (*status.LustreClient, error) {
	var buffer [2048]C.char
	rc, err := C.llapi_getname(C.CString(mountPath), &buffer[0], C.size_t(len(buffer)))
	if rc < 0 || err != nil {
		return nil, fmt.Errorf("lustre:  %v %d %v", mountPath, rc, err)
	}
	id := C.GoString(&buffer[0])
	elem := strings.Split(id, "-")
	c := status.LustreClient{FsName: elem[0], ClientID: elem[1]}
	return &c, nil
}

// RootDir represent a the mount point of a Lustre filesystem.
type RootDir string

// Join args with root dir to create an absolute path.
// FIXME: replace this with OpenAt and friends
func (root RootDir) Join(args ...string) string {
	return path.Join(string(root), path.Join(args...))
}

func (root RootDir) String() string {
	return string(root)
}

// Path returns the path for the root
func (root RootDir) Path() string {
	return string(root)
}

// FilesystemID should be a unique identifier for a filesystem. For now just use RootDir
type FilesystemID RootDir

func (root FilesystemID) String() string {
	return string(root)
}

// Path returns the path for the root
func (root FilesystemID) Path() (string, error) {
	return string(root), nil
}

// GetID returns the filesystem's ID. For the moment, this is the root path, but in
// the future it could be something more globally unique (uuid?).
func GetID(p string) (FilesystemID, error) {
	r, err := MountRoot(p)
	if err != nil {
		return FilesystemID(r), err
	}
	return FilesystemID(r), nil
}

// Determine if given directory is the one true magical DOT_LUSTRE directory.
func isDotLustre(dir string) bool {
	fi, err := os.Lstat(dir)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		fid, err := LookupFid(dir)
		if err == nil && fid.IsDotLustre() {
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
		}
		return ""
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
		}
		return "", ""
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

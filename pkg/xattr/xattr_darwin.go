package xattr

import (
	"errors"
	"syscall"
	"unsafe"
)

var _zero uintptr

// Lgetxattr returns the extended attribute from the path name.
func Lgetxattr(path, attr string, dest []byte) (sz int, err error) {
	pathBuf, err := syscall.BytePtrFromString(path)
	if err != nil {
		return
	}

	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}

	var buf unsafe.Pointer
	if len(dest) > 0 {
		buf = unsafe.Pointer(&dest[0])
	} else {
		buf = unsafe.Pointer(&_zero)
	}

	rc, _, errno := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(pathBuf)),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(buf),
		uintptr(len(dest)),
		0,
		syscall.XATTR_NOFOLLOW)

	sz = int(rc)
	if errno != 0 {
		err = errno
	}
	return
}

// Lsetxattr sets the extended attribute on the path name
func Lsetxattr(path, attr string, value []byte) ([]byte, error) {
	pathBuf, err := syscall.BytePtrFromString(path)
	if err != nil {
		return err
	}

	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return err
	}

	valuePtr := &value[0]

	_, _, errno := syscall.Syscall6(syscall.SYS_SETXATTR,
		uintptr(unsafe.Pointer(pathBuf)),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(unsafe.Pointer(valuePtr)),
		uintptr(len(value)),
		0,
		flags|syscall.XATTR_NOFOLLOW)
	if errno == 0 {
		return nil
	}
	return errno
}

func Fsetxattr(fd int, attr string, value []byte, flags int) error {
	return errors.New("Fsetxattr unimplemented on this platform.")
}

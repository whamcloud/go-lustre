package xattr

// #include <sys/xattr.h>
import "C"
import (
	"syscall"
	"unsafe"
)

const (
	CREATE  = C.XATTR_CREATE
	REPLACE = C.XATTR_REPLACE
)

var _zero uintptr

// Lgetxattr returns the extended attribute from the path name.
func Lgetxattr(path, attr string) ([]byte, error) {
	pathBuf, err := syscall.BytePtrFromString(path)
	if err != nil {
		return nil, err
	}

	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 128)
	sz, _, errno := syscall.Syscall6(syscall.SYS_LGETXATTR,
		uintptr(unsafe.Pointer(pathBuf)),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
		0,
		0)

	if errno != 0 {
		return nil, errno
	}
	return buf[:sz], nil
}

// Lsetxattr sets the extended attribute on the path name
func Lsetxattr(path, attr string, value []byte, flags int) (err error) {
	pathBuf, err := syscall.BytePtrFromString(path)
	if err != nil {
		return
	}

	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return
	}

	var valuePtr unsafe.Pointer
	if len(value) > 0 {
		valuePtr = unsafe.Pointer(&value[0])
	} else {
		valuePtr = unsafe.Pointer(&_zero)
	}
	_, _, errno := syscall.Syscall6(syscall.SYS_LSETXATTR,
		uintptr(unsafe.Pointer(pathBuf)),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(valuePtr),
		uintptr(len(value)),
		uintptr(flags),
		0)
	if errno != 0 {
		err = errno
	}
	return
}

// Lsetxattr sets the extended attribute on the path name
func Fsetxattr(fd uintptr, attr string, value []byte, flags int) error {
	attrBuf, err := syscall.BytePtrFromString(attr)
	if err != nil {
		return err
	}

	valuePtr := &value[0]

	_, _, errno := syscall.Syscall6(syscall.SYS_FSETXATTR,
		fd,
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(unsafe.Pointer(valuePtr)),
		uintptr(len(value)),
		uintptr(flags),
		0)
	if errno == 0 {
		return nil
	}
	return errno
}

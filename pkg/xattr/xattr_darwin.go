package xattr

import (
	"errors"
	"syscall"
	"unsafe"
)

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
	sz, _, errno := syscall.Syscall6(syscall.SYS_GETXATTR,
		uintptr(unsafe.Pointer(pathBuf)),
		uintptr(unsafe.Pointer(attrBuf)),
		uintptr(unsafe.Pointer(&buf[0])),
		uintptr(len(buf)),
		0,
		syscall.XATTR_NOFOLLOW)

	if errno != 0 {
		switch errno {
		case syscall.ENODATA:
			return nil, errno
		case syscall.ENOTSUP:
			return nil, errno
		case syscall.ERANGE:
			return nil, errno
		default:
			return nil, errno
		}
	}
	return buf[:sz], nil
}

// Lsetxattr sets the extended attribute on the path name
func Lgetxattr(path, attr string) ([]byte, error) {
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

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

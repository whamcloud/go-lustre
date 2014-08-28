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
	"unsafe"
)

// Version returns the current Lustre version string.
func Version() string {
	var buffer [8182]byte
	var cversion *C.char
	var version string
	_, err := C.llapi_get_version((*C.char)(unsafe.Pointer(&buffer[0])), C.int(len(buffer)),
		&cversion)
	if err != nil {
		fmt.Println(err)
		return ""
	}

	version = C.GoString(cversion)
	return version
}

package llapi

/*
#include <lustre/lustreapi.h>
#include <stdlib.h>
*/
import "C"
import (
	"unsafe"
)

// Creates a file with the given lustre attributes, this closes the filedescriptor once it is done.
func CreateFile(name string, stripe_count, stripe_size, stripe_offset, stripe_pattern int) error {
	tfile := C.CString(name)
	defer C.free(unsafe.Pointer(tfile))
	rc, err := C.llapi_file_create(tfile, (C.ulonglong)(stripe_size), (C.int)(stripe_offset),
		(C.int)(stripe_count), (C.int)(stripe_pattern))
	if err := isError(rc, err); err != nil {
		return err
	}
	return nil
}

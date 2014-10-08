package lustre

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
// #include <stdlib.h>
//
import "C"

import (
	"fmt"
	"unsafe"
)

// Request submits an HSM request for list of files
// The max suported size of the fileList is about 50.
func Request(fs_root string, cmd uint, archiveId uint, fileList []string) (int, error) {
	hurLen := C.int(len(fileList))
	hur := C.llapi_hsm_user_request_alloc(hurLen, 0)
	defer C.free(unsafe.Pointer(hur))

	hur.hur_request.hr_action = C.uint(cmd)
	hur.hur_request.hr_archive_id = C.uint(archiveId)
	hur.hur_request.hr_flags = 0
	hur.hur_request.hr_data_len = 0

	userItemSize := unsafe.Sizeof(C.struct_hsm_user_item{})
	start := uintptr(unsafe.Pointer(&hur.hur_user_item))
	for i, file := range fileList {
		fid, err := LookupFid(file)
		if err != nil {
			fmt.Printf("%s: unable to lookup fid (%s)", file, err)
			continue
		}
		huiPos := start + uintptr(i)*userItemSize
		hui := (*C.struct_hsm_user_item)(unsafe.Pointer(huiPos))
		hui.hui_extent.offset = 0
		hui.hui_extent.length = C.ulonglong(^uint(0))
		hui.hui_fid = C.lustre_fid(*fid)
		hur.hur_request.hr_itemcount++
	}
	num := int(hur.hur_request.hr_itemcount)
	rc, err := C.llapi_hsm_request(C.CString(fs_root), hur)
	if rc < 0 || err != nil {
		return num, err
	}
	return num, nil
}

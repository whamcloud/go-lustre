package lustre

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
// #include <stdlib.h>
//
import "C"

import (
	"fmt"
	"reflect"
	"unsafe"
)

// RequestHsmArchive submits a request to the coordinator for the
// specified list of fids to be archived to the specfied archive id.
func RequestHsmArchive(fsID FilesystemID, archiveID uint, fids []Fid) error {
	return hsmRequest(fsID, C.HUA_ARCHIVE, archiveID, fids)
}

// RequestHsmRestore submits a request to the coordinator for the
// specified list of fids to be restored from the specfied archive id.
func RequestHsmRestore(fsID FilesystemID, archiveID uint, fids []Fid) error {
	return hsmRequest(fsID, C.HUA_RESTORE, archiveID, fids)
}

// RequestHsmRelease submits a request to the coordinator for the
// specified list of fids to be released.
func RequestHsmRelease(fsID FilesystemID, archiveID uint, fids []Fid) error {
	return hsmRequest(fsID, C.HUA_RELEASE, archiveID, fids)
}

// RequestHsmRemove submits a request to the coordinator for the
// specified list of fids to be removed from the HSM backend.
func RequestHsmRemove(fsID FilesystemID, archiveID uint, fids []Fid) error {
	return hsmRequest(fsID, C.HUA_REMOVE, archiveID, fids)
}

// RequestHsmCancel submits a request to the coordinator to cancel any
// outstanding requests involving the specified list of fids.
func RequestHsmCancel(fsID FilesystemID, archiveID uint, fids []Fid) error {
	return hsmRequest(fsID, C.HUA_CANCEL, archiveID, fids)
}

func hsmRequest(fsID FilesystemID, cmd uint, archiveID uint, fids []Fid) error {
	mnt, err := fsID.Path()
	if err != nil {
		return err
	}

	if _, err = request(mnt, cmd, archiveID, fids); err != nil {
		return err
	}
	return nil
}

// Request submits an HSM request for list of files
// The max suported size of the fileList is about 50.
func request(r string, cmd uint, archiveID uint, fids []Fid) (int, error) {
	fileCount := len(fids)
	if fileCount < 1 {
		return 0, fmt.Errorf("Request must include at least 1 file!")
	}

	hur := C.llapi_hsm_user_request_alloc(C.int(fileCount), 0)
	defer C.free(unsafe.Pointer(hur))
	if hur == nil {
		panic("Failed to allocate HSM User Request struct!")
	}

	hur.hur_request.hr_action = C.__u32(cmd)
	hur.hur_request.hr_archive_id = C.__u32(archiveID)
	hur.hur_request.hr_flags = 0
	hur.hur_request.hr_itemcount = 0
	hur.hur_request.hr_data_len = 0

	// https://code.google.com/p/go-wiki/wiki/cgo#Turning_C_arrays_into_Go_slices
	hdr := reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(&hur.hur_user_item)),
		Len:  fileCount,
		Cap:  fileCount,
	}
	userItems := *(*[]C.struct_hsm_user_item)(unsafe.Pointer(&hdr))
	for i, f := range fids {
		userItems[i].hui_extent.offset = 0
		userItems[i].hui_extent.length = C.__u64(^uint(0))
		userItems[i].hui_fid = *f.(*fid).cfid
		hur.hur_request.hr_itemcount++
	}

	num := int(hur.hur_request.hr_itemcount)
	if num != fileCount {
		return 0, fmt.Errorf("lustre: Can't submit incomplete request (%d/%d)", num, fileCount)
	}

	rc, err := C.llapi_hsm_request(C.CString(r), hur)
	if rc != 0 || err != nil {
		if err != nil {
			return 0, fmt.Errorf("lustre: Got error from llapi_hsm_request: %s", err.Error())
		}
		return 0, fmt.Errorf("lustre: Got rc %d from llapi_hsm_request()", rc)
	}
	return num, nil
}

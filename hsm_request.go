package lustre

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
// #include <stdlib.h>
//
import "C"

import (
	"fmt"
	"path"
	"reflect"
	"strings"
	"unsafe"
)

func RequestHsmArchive(fsID FilesystemID, archiveId uint, fileList []string) error {
	return hsmRequest(fsID, C.HUA_ARCHIVE, archiveId, fileList)
}

func RequestHsmRestore(fsID FilesystemID, archiveId uint, fileList []string) error {
	return hsmRequest(fsID, C.HUA_RESTORE, archiveId, fileList)
}

func RequestHsmRelease(fsID FilesystemID, archiveId uint, fileList []string) error {
	return hsmRequest(fsID, C.HUA_RELEASE, archiveId, fileList)
}

func RequestHsmRemove(fsID FilesystemID, archiveId uint, fileList []string) error {
	return hsmRequest(fsID, C.HUA_REMOVE, archiveId, fileList)
}

func RequestHsmCancel(fsID FilesystemID, archiveId uint, fileList []string) error {
	return hsmRequest(fsID, C.HUA_CANCEL, archiveId, fileList)
}

func hsmRequest(fsID FilesystemID, cmd uint, archiveId uint, fileList []string) error {
	mnt, err := fsID.Path()
	if err != nil {
		return err
	}

	if _, err = Request(mnt, cmd, archiveId, fileList); err != nil {
		return err
	}
	return nil
}

// Request submits an HSM request for list of files
// The max suported size of the fileList is about 50.
func Request(fs_root string, cmd uint, archiveId uint, inList []string) (int, error) {
	// Make a copy so that we don't modify the supplied list.
	fileList := make([]string, len(inList))
	copy(fileList, inList)
	for idx, filePath := range fileList {
		if !strings.HasPrefix(filePath, fs_root) {
			fileList[idx] = path.Join(fs_root, filePath)
		}
	}

	fileCount := len(fileList)
	if fileCount < 1 {
		return 0, fmt.Errorf("Request must include at least 1 file!")
	}

	hur := C.llapi_hsm_user_request_alloc(C.int(fileCount), 0)
	defer C.free(unsafe.Pointer(hur))
	if hur == nil {
		panic("Failed to allocate HSM User Request struct!")
	}

	hur.hur_request.hr_action = C.__u32(cmd)
	hur.hur_request.hr_archive_id = C.__u32(archiveId)
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
	for i, file := range fileList {
		fid, err := LookupFid(file)
		if err != nil {
			fmt.Printf("%s: unable to lookup fid (%s)", file, err)
			continue
		}
		userItems[i].hui_extent.offset = 0
		userItems[i].hui_extent.length = C.__u64(^uint(0))
		userItems[i].hui_fid = C.lustre_fid(fid)
		hur.hur_request.hr_itemcount++
	}

	num := int(hur.hur_request.hr_itemcount)
	if num != fileCount {
		return 0, fmt.Errorf("lustre: Can't submit incomplete request (%d/%d)", num, fileCount)
	}

	rc, err := C.llapi_hsm_request(C.CString(fileList[0]), hur)
	if rc != 0 || err != nil {
		if err != nil {
			return 0, fmt.Errorf("lustre: Got error from llapi_hsm_request: %s", err.Error())
		} else {
			return 0, fmt.Errorf("lustre: Got rc %d from llapi_hsm_request()", rc)
		}
	}
	return num, nil
}

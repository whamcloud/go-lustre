package llapi

//
// #include <sys/types.h>
// #include <sys/stat.h>
// #include <unistd.h>
// #include <lustre/lustreapi.h>
// #include <stdlib.h>
//
import "C"

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
)

var errStatError = errors.New("stat failure")

func statToCstat(fi os.FileInfo) *C.struct_stat {
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		debug.Print("no stat info")
		return nil
	}

	var st C.struct_stat
	st.st_uid = C.__uid_t(stat.Uid)
	st.st_gid = C.__gid_t(stat.Gid)
	st.st_mode = C.__mode_t(stat.Mode)
	st.st_size = C.__off_t(stat.Size)
	st.st_mtim.tv_sec = C.__time_t(stat.Mtim.Sec)
	st.st_mtim.tv_nsec = C.__syscall_slong_t(stat.Mtim.Nsec)
	st.st_atim.tv_sec = C.__time_t(stat.Atim.Sec)
	st.st_atim.tv_nsec = C.__syscall_slong_t(stat.Atim.Nsec)

	return &st
}

// HsmImport creates a placeholder file in Lustre that refers to the
// file contents stored in an HSM backend.  The file is created in the
// "released" state, and the contents will be retrieved when the file is opened
// or an explicit restore is requested.
//
// TODO: using an os.FileInfo to pass the file metadata doesn't work for all cases. This
// should be simple struct the caller can populate. (Though just using syscall.Stat_t
// is also tempting.)
func HsmImport(
	name string,
	archive uint,
	fi os.FileInfo,
	stripeSize uint64,
	stripeOffset int,
	stripeCount int,
	stripePattern int,
	poolName string) (*lustre.Fid, error) {

	var cfid C.lustre_fid

	st := statToCstat(fi)
	if st == nil {
		return nil, errStatError
	}

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	rc, err := C.llapi_hsm_import(
		cname,
		C.int(archive),
		st,
		C.ulonglong(stripeSize),
		C.int(stripeOffset),
		C.int(stripeCount),
		C.int(stripePattern),
		nil,
		&cfid,
	)
	if rc < 0 {
		return nil, err
	}
	return fromCFid(&cfid), nil
}

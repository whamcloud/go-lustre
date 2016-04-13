package llapi

/*
// #include <sys/types.h>
// #include <sys/stat.h>
// #include <unisted.h>
#include <fcntl.h>      // Needed for C.O_LOV_DELAY_CREATE definition
#include <sys/ioctl.h>  // Needed for LL_IOC_HSM_IMPORT definition
#include <stdlib.h>
#include <lustre/lustreapi.h>

void lum_set_stripe_offset(struct lov_user_md_v3 *lum, __u16 offset) {
	lum->lmm_stripe_offset = offset;
}
*/
import "C"

import (
	"errors"
	"os"
	"syscall"
	"unsafe"

	"github.intel.com/hpdd/logging/debug"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/luser"
	"github.intel.com/hpdd/lustre/pkg/xattr"
	"golang.org/x/sys/unix"
)

var errStatError = errors.New("stat failure")

// The nsec fields in stat_t are defined differently between EL6 and
// EL7, and Go's C compiler complains. Worked around this by
// reimplmenting HsmImport in Go, below. Leaving original code here
// as a reference and in case this doesn't work out.

/*
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
*/

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

	return hsmImport(name, archive, fi, stripeSize, stripeOffset, stripeCount, stripePattern, poolName)

	// Orignal llapi version
	/*
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
	*/
}

func lovUserMdSize(stripes int, lmm_magic uint32) int {
	if lmm_magic == C.LOV_USER_MAGIC_V1 {
		return sizeof(C.struct_lov_user_md_v1{}) +
			stripes*sizeof(C.struct_lov_user_ost_data_v1{})
	}
	return sizeof(C.struct_lov_user_md_v3{}) +
		stripes*sizeof(C.struct_lov_user_ost_data_v1{})
}

// Experiemental native go implementation of llapi_hsm_import.
// The llapi call has been "unrolled" into one big ugly func.
// CGO is still needed for C types, strncpy, and a
// setter func for a union field, but no llapi funcs were used.
func hsmImport(
	name string,
	archive uint,
	fi os.FileInfo,
	stripeSize uint64,
	stripeOffset int,
	stripeCount int,
	stripePattern int,
	poolName string) (*lustre.Fid, error) {

	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		debug.Print("no stat info")
		return nil, errStatError
	}

	fd, err := unix.Open(name, unix.O_CREAT|unix.O_WRONLY|C.O_LOV_DELAY_CREATE, stat.Mode)
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)

	fid, err := luser.GetFidFd(fd)
	if err != nil {
		return nil, err
	}

	// setstripe
	maxLumSize := lovUserMdSize(C.LOV_MAX_STRIPE_COUNT, C.LOV_USER_MAGIC_V3)
	buf := make([]byte, maxLumSize)
	lum := (*C.struct_lov_user_md_v3)(unsafe.Pointer(&buf[0]))

	if stripePattern == 0 {
		stripePattern = C.LOV_PATTERN_RAID0
	}

	lum.lmm_magic = C.LOV_USER_MAGIC_V3
	lum.lmm_pattern = C.__u32(stripePattern | C.LOV_PATTERN_F_RELEASED)
	lum.lmm_stripe_size = C.__u32(stripeSize)
	lum.lmm_stripe_count = C.__u16(stripeCount)
	C.lum_set_stripe_offset(lum, C.__u16(stripeOffset))
	if poolName != "" {
		cpool := C.CString(poolName)
		C.strncpy((*C.char)(unsafe.Pointer(&lum.lmm_pool_name[0])), cpool, C.LOV_MAXPOOLNAME)
		C.free(unsafe.Pointer(cpool))
	}

	lumSize := lovUserMdSize(0, uint32(lum.lmm_magic))

	err = xattr.Fsetxattr(fd, "lustre.lov", buf[:lumSize], xattr.CREATE)
	if err != nil {
		return nil, err
	}

	var hui C.struct_hsm_user_import
	hui.hui_uid = C.__u32(stat.Uid)
	hui.hui_gid = C.__u32(stat.Gid)
	hui.hui_mode = C.__u32(stat.Mode)
	hui.hui_size = C.__u64(stat.Size)
	hui.hui_mtime = C.__u64(stat.Mtim.Sec)
	hui.hui_mtime_ns = C.__u32(stat.Mtim.Nsec)
	hui.hui_atime = C.__u64(stat.Atim.Sec)
	hui.hui_atime_ns = C.__u32(stat.Atim.Nsec)
	hui.hui_archive_id = C.__u32(archive)

	rc, err := ioctl(fd, C.LL_IOC_HSM_IMPORT, uintptr(unsafe.Pointer(&hui)))
	if rc < 0 || err != nil {
		return nil, err
	}
	return fid, nil
}

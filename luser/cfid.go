// Package luser uses lustre interfaces exported to usersapce directly, instead of using the liblustreapi.a library.
//
package luser

//
// #include <stdlib.h>
// #include <lustre/lustre_user.h>
//
import "C"
import (
	"unsafe"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/system"
)

func fromCFid(f *C.struct_lu_fid) *lustre.Fid {
	return &lustre.Fid{
		Seq: uint64(f.f_seq),
		Oid: uint32(f.f_oid),
		Ver: uint32(f.f_ver),
	}
}

func toCFid(fid *lustre.Fid) *C.struct_lu_fid {
	return &C.struct_lu_fid{
		f_seq: C.__u64(fid.Seq),
		f_oid: C.__u32(fid.Oid),
		f_ver: C.__u32(fid.Ver),
	}
}

const XATTR_NAME_LMA = "trusted.lma" // copied from lustre_idl.h

// GetFid retuns the lustre.Fid for the path name.
func GetFid(path string) (*lustre.Fid, error) {
	buf, err := system.Lgetxattr(path, XATTR_NAME_LMA)
	if err != nil {
		return nil, err
	}
	lma := (*C.struct_lustre_mdt_attrs)(unsafe.Pointer(&buf[0]))
	return fromCFid(&lma.lma_self_fid), nil
}

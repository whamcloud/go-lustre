// Package luser uses lustre interfaces exported to usersapce
// directly, instead of using the liblustreapi.a library.
// Data structures created mirror those defined in lustre_user.h

package luser

import (
	"encoding/binary"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/pkg/xattr"
)

const XATTR_NAME_LMA = "trusted.lma" // from lustre_idl.h

// GetFid retuns the lustre.Fid for the path name.
func GetFid(path string) (*lustre.Fid, error) {
	buf := make([]byte, 64)
	_, err := xattr.Lgetxattr(path, XATTR_NAME_LMA, buf)
	if err != nil {
		return nil, err
	}
	// fid is buf + 8 offset
	fid := parseFid(buf[8:24], binary.LittleEndian)
	return &fid, nil
}

// GetFid retuns the lustre.Fid for the path name.
func GetFidFd(fd int) (*lustre.Fid, error) {
	buf := make([]byte, 64)
	_, err := xattr.Fgetxattr(fd, XATTR_NAME_LMA, buf)
	if err != nil {
		return nil, err
	}
	fid := parseFid(buf[8:24], binary.LittleEndian)
	return &fid, nil
}

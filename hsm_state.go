package lustre

/*
#cgo LDFLAGS: -llustreapi
#include <lustre/lustreapi.h>
#include <stdlib.h>

// This doesn't exist in the API, but maybe it should?
struct hsm_user_state *hsm_user_state_alloc()
{
	int len = 0;

	len += sizeof(struct hsm_user_state);
	len += sizeof(struct hsm_extent);

	return (struct hsm_user_state *)malloc(len);
}
*/
import "C"

import (
	"bytes"
	"fmt"
	"unsafe"
)

// HsmState is the hsm state for a file.
type HsmState struct {
	states    uint32
	archiveID uint32
}

// Exists is true if the HSM has been enabled for a file. A copy or partial copy of the file
// may exist in the backend. Or it might not.
func (s *HsmState) Exists() bool {
	return s.states&C.HS_EXISTS > 0
}

// Archived is true of a completly (but possibly stale) copy of the file contents are stored in the archive.
func (s *HsmState) Archived() bool {
	return s.states&C.HS_ARCHIVED > 0
}

// Dirty is true if the file has been modified since the last time it was archived.
func (s *HsmState) Dirty() bool {
	return s.states&C.HS_DIRTY > 0
}

// Released is true if the contents of the file have been removed from the filesystem. Only
// possible if the file has been Archived.
func (s *HsmState) Released() bool {
	return s.states&C.HS_RELEASED > 0
}

// NoRelease flag prevents the file data from being relesed, even if it is Archived.
func (s *HsmState) NoRelease() bool {
	return s.states&C.HS_NORELEASE > 0
}

// NoArchive flag inhibits archiving the file. (Useful for temporary files perhaps.)
func (s *HsmState) NoArchive() bool {
	return s.states&C.HS_NOARCHIVE > 0
}

// Lost flag means the copy of the file in the archive is not accessible.
func (s *HsmState) Lost() bool {
	return s.states&C.HS_LOST > 0
}

// ArchiveID is the id of the archive associated with this file.
func (s *HsmState) ArchiveID() uint32 {
	return s.archiveID
}

func (s *HsmState) String() string {
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("%#x", s.states))

	if s.Released() {
		buffer.WriteString(" released")
	}
	if s.Exists() {
		buffer.WriteString(" exists")
	}
	if s.Dirty() {
		buffer.WriteString(" dirty")
	}
	if s.Archived() {
		buffer.WriteString(" archived")
	}
	if s.NoRelease() {
		buffer.WriteString(" never_release")
	}
	if s.NoArchive() {
		buffer.WriteString(" never_archive")
	}
	if s.Lost() {
		buffer.WriteString(" lost_from_hsm")
	}
	return buffer.String()
}

// GetHsmState returns the HSM state for the given file.
func GetHsmState(filePath string) (*HsmState, error) {
	hus := C.hsm_user_state_alloc()
	defer C.free(unsafe.Pointer(hus))

	rc, err := C.llapi_hsm_state_get(C.CString(filePath), hus)
	if err != nil {
		return nil, err
	}
	if rc > 0 {
		return nil, fmt.Errorf("Got %d from llapi_hsm_state, expected 0", rc)
	}
	return &HsmState{uint32(hus.hus_states), uint32(hus.hus_archive_id)}, nil
}

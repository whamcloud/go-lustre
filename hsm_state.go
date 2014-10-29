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
	"errors"
	"fmt"
	"unsafe"
)

type HsmState struct {
	Path      string
	States    uint32
	ArchiveId uint32
}

func (s *HsmState) Exists() bool {
	return s.States&C.HS_EXISTS > 0
}

func (s *HsmState) Dirty() bool {
	return s.States&C.HS_DIRTY > 0
}

func (s *HsmState) Released() bool {
	return s.States&C.HS_RELEASED > 0
}

func (s *HsmState) Archived() bool {
	return s.States&C.HS_ARCHIVED > 0
}

func (s *HsmState) NoRelease() bool {
	return s.States&C.HS_NORELEASE > 0
}

func (s *HsmState) NoArchive() bool {
	return s.States&C.HS_NOARCHIVE > 0
}

func (s *HsmState) Lost() bool {
	return s.States&C.HS_LOST > 0
}

func (s *HsmState) String() string {
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("%s: %#x", s.Path, s.States))

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

func GetHsmState(filePath string) (*HsmState, error) {
	hus := C.hsm_user_state_alloc()
	defer C.free(unsafe.Pointer(hus))

	rc, err := C.llapi_hsm_state_get(C.CString(filePath), hus)
	if err != nil {
		return nil, err
	}
	if rc > 0 {
		return nil, errors.New(fmt.Sprintf("Got %d from llapi_hsm_state, expected 0", rc))
	}
	return &HsmState{filePath, uint32(hus.hus_states), uint32(hus.hus_archive_id)}, nil
}

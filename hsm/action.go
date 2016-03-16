package hsm

/*
#cgo LDFLAGS: -llustreapi
#include <lustre/lustreapi.h>
#include <stdlib.h>

struct lov_user_md *  lum_fix_lov_ea(struct lov_user_md_v1 * lum) {
        lum->lmm_pattern = lum->lmm_pattern ^ LOV_PATTERN_F_RELEASED;
        lum->lmm_stripe_offset = -1;
        return lum;
}

*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"

	"github.intel.com/hpdd/logging/alert"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/llapi"
	"github.intel.com/hpdd/lustre/pkg/xattr"
	"golang.org/x/sys/unix"
)

// Expose the internal constants for external users
const (
	NONE    = llapi.HsmActionNone
	ARCHIVE = llapi.HsmActionArchive
	RESTORE = llapi.HsmActionRestore
	REMOVE  = llapi.HsmActionRemove
	CANCEL  = llapi.HsmActionCancel
)

type (
	// Coordinator receives HSM actions to execute.
	Coordinator struct {
		root fs.RootDir
		hcp  *llapi.HsmCopytoolPrivate
	}

	// ActionItem is one action to perform on specified file.
	actionItem struct {
		mu        sync.Mutex
		cdt       *Coordinator
		hcap      *llapi.HsmCopyActionPrivate
		hai       llapi.HsmActionItem
		halFlags  uint64
		archiveID uint
	}

	// actionItemHandle is an "open" actionItem that is currrently being processed.
	actionItemHandle actionItem

	// ErrIOError are errors that returned by the HSM library.
	ErrIOError struct {
		msg string
	}
)

func (e ErrIOError) Error() string {
	return e.msg
}

// IoError returns a new error.
func IoError(msg string) error {
	return errors.New(msg)
}

// CoordinatorConnection opens a connection to the coordinator.
func CoordinatorConnection(path fs.RootDir, nonBlocking bool) (*Coordinator, error) {
	var cdt = Coordinator{root: path}
	var err error

	flags := llapi.CopytoolDefault

	if nonBlocking {
		flags = llapi.CopytoolNonBlock
	}

	cdt.hcp, err = llapi.HsmCopytoolRegister(path.String(), 0, nil, flags)
	if err != nil {
		return nil, err
	}
	return &cdt, nil
}

// Recv blocks and waits for new action items from the coordinator.
// Retuns a slice of *actionItem.
func (cdt *Coordinator) Recv() ([]*actionItem, error) {

	if cdt.hcp == nil {
		return nil, errors.New("coordinator closed")
	}
	actionList, err := llapi.HsmCopytoolRecv(cdt.hcp)
	if err != nil {
		return nil, err
	}
	items := make([]*actionItem, len(actionList.Items))
	for i, hai := range actionList.Items {
		item := &actionItem{
			halFlags:  actionList.Flags,
			archiveID: actionList.ArchiveID,
			cdt:       cdt,
			hai:       hai,
		}
		items[i] = item
	}
	return items, nil
}

//GetFd returns copytool file descriptor
func (cdt *Coordinator) GetFd() int {
	return llapi.HsmCopytoolGetFd(cdt.hcp)
}

// Close terminates connection with coordinator.
func (cdt *Coordinator) Close() {
	if cdt.hcp != nil {
		llapi.HsmCopytoolUnregister(&cdt.hcp)
		cdt.hcp = nil
	}
}

type (
	// ActionRequest is an HSM action
	ActionRequest interface {
		Begin(openFlags int, isError bool) (ActionHandle, error)
		FailImmediately(errval int)
		ArchiveID() uint
		String() string
	}

	// ActionHandle is an HSM action that is currently being processed
	ActionHandle interface {
		Progress(offset uint64, length uint64, totalLength uint64, flags int) error
		End(offset uint64, length uint64, flags int, errval int) error
		Action() llapi.HsmAction
		Fid() *lustre.Fid
		Cookie() uint64
		DataFid() (*lustre.Fid, error)
		Fd() (uintptr, error)
		Offset() uint64
		ArchiveID() uint
		Length() uint64
		String() string
		Data() []byte
	}
)

// Copy the striping info from the primary to the temporary file.
//
// This needs to be in once place because we're copying the same C
// structure from llapi_file_get_stripe to fsetxattr, and Go won't
// let a C type sharing between packages.
// Once llpai/layout is fixed we can use that.
//
func (aih *actionItemHandle) copyLovMd() error {
	src := fs.FidPath(aih.cdt.root, aih.Fid())
	cSrc := C.CString(src)
	defer C.free(unsafe.Pointer(cSrc))

	maxLumSize := C.lov_user_md_size(C.LOV_MAX_STRIPE_COUNT, C.LOV_USER_MAGIC_V3)
	buf := make([]byte, maxLumSize)
	lum := (*C.struct_lov_user_md)(unsafe.Pointer(&buf[0]))

	rc, err := C.llapi_file_get_stripe(cSrc, lum)
	if err != nil {
		return err
	}
	if rc < 0 {
		return errors.New("null lum")
	}

	C.lum_fix_lov_ea(lum)

	lumSize := C.lov_user_md_size(0, lum.lmm_magic)

	fd, err := aih.Fd()
	if err != nil {
		return err
	}
	defer unix.Close(int(fd))
	err = xattr.Fsetxattr(fd, "lustre.lov", buf[:lumSize], xattr.CREATE)
	if err != nil {
		return err
	}
	return nil

}

// Begin prepares an actionItem for processing.
//
// returns an actionItemHandle. The End method must be called to complete
// this action.
func (ai *actionItem) Begin(openFlags int, isError bool) (ActionHandle, error) {
	mdtIndex := -1
	setLov := false
	if ai.Action() == RESTORE && !isError {
		var err error
		mdtIndex, err = fs.GetMdt(ai.cdt.root, ai.Fid())
		if err != nil {

			return nil, err
		}
		openFlags = llapi.LovDelayCreate
		setLov = true
	}
	var err error
	ai.mu.Lock()
	ai.hcap, err = llapi.HsmActionBegin(ai.cdt.hcp, &ai.hai, mdtIndex, openFlags, isError)
	ai.mu.Unlock()
	if err != nil {
		ai.mu.Lock()
		llapi.HsmActionEnd(&ai.hcap, 0, 0, 0, -1)
		ai.mu.Unlock()
		return nil, err

	}
	aih := (*actionItemHandle)(ai)
	if setLov {
		if err := aih.copyLovMd(); err != nil {
			alert.Warn(err)
		}
	}
	return aih, nil
}

func (ai *actionItem) String() string {
	return (*actionItemHandle)(ai).String()
}

// ArchiveID returns the archive id associated with teh actionItem.
func (ai *actionItem) ArchiveID() uint {
	return ai.archiveID
}

// Action returns name of the action.
func (ai *actionItem) Action() llapi.HsmAction {
	return ai.hai.Action
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *actionItem) Fid() *lustre.Fid {
	return ai.hai.Fid
}

// FailImmediately completes the ActinoItem with given error.
// The passed actionItem is no longer valid when this function returns.
func (ai *actionItem) FailImmediately(errval int) {
	aih, err := ai.Begin(0, true)
	if err != nil {
		return
	}
	aih.End(0, 0, 0, errval)
}

func lengthStr(length uint64) string {
	if length == ^uint64(0) {
		return "EOF"
	}
	return fmt.Sprintf("%d", length)
}

func (ai *actionItemHandle) String() string {
	return fmt.Sprintf("AI: %x %v %v %d,%v", ai.hai.Cookie, ai.Action(), ai.Fid(), ai.Offset(), lengthStr(ai.Length()))
}

// Progress reports current progress of an action.
func (ai *actionItemHandle) Progress(offset uint64, length uint64, totalLength uint64, flags int) error {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	return llapi.HsmActionProgress(ai.hcap, offset, length, totalLength, flags)
}

// End completes an action with specified status.
// No more requests should be made on this action after calling this.
func (ai *actionItemHandle) End(offset uint64, length uint64, flags int, errval int) error {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	return llapi.HsmActionEnd(&ai.hcap, offset, length, flags, errval)
}

// Action returns name of the action.
func (ai *actionItemHandle) Action() llapi.HsmAction {
	return ai.hai.Action
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *actionItemHandle) Fid() *lustre.Fid {
	return ai.hai.Fid
}

// Cookie returns the action identifier.
func (ai *actionItemHandle) Cookie() uint64 {
	return ai.hai.Cookie
}

// DataFid returns the FID of the data file.
// This file should be used for all Lustre IO for archive and restore commands.
func (ai *actionItemHandle) DataFid() (*lustre.Fid, error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	return llapi.HsmActionGetDataFid(ai.hcap)
}

// Fd returns the file descriptor of the DataFid.
// If used, this Fd must be closed prior to calling End.
func (ai *actionItemHandle) Fd() (uintptr, error) {
	ai.mu.Lock()
	defer ai.mu.Unlock()
	fd, err := llapi.HsmActionGetFd(ai.hcap)
	if err != nil {
		return 0, err
	}
	return fd, nil
}

// Offset returns the offset for the action.
func (ai *actionItemHandle) Offset() uint64 {
	return uint64(ai.hai.Extent.Offset)
}

// Length returns the length of the action request.
func (ai *actionItemHandle) Length() uint64 {
	return uint64(ai.hai.Extent.Length)
}

// ArchiveID returns archive for this action.
// Duplicating this on the action allows actions to be
// self-contained.
func (ai *actionItemHandle) ArchiveID() uint {
	return ai.archiveID
}

// Data returns the additional request data.
// The format of the data is agreed upon by the initiator of the HSM
// request and backend driver that is doing the work.
func (ai *actionItemHandle) Data() []byte {
	return ai.hai.Data
}

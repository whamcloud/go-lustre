package lustre

//
// #cgo LDFLAGS: -llustreapi
// #include <fcntl.h>
// #include <lustre/lustreapi.h>
//
import "C"

import (
	"errors"
	"fmt"

	"github.com/golang/glog"
)

type (
	// Coordinator receives HSM actions to execute.
	Coordinator struct {
		root RootDir
		hcp  *C.struct_hsm_copytool_private
	}

	// ActionItem is one action to perform on specified file.
	ActionItem struct {
		cdt       *Coordinator
		hai       C.struct_hsm_action_item
		hcap      *C.struct_hsm_copyaction_private
		halFlags  uint64
		archiveID uint
	}

	// ActionItemHandle is an "open" actionItem that is currrently being processed.
	ActionItemHandle ActionItem

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
func CoordinatorConnection(path RootDir, nonBlocking bool) (*Coordinator, error) {
	var cdt = Coordinator{root: path}
	var flags C.int

	if nonBlocking {
		flags = C.O_NONBLOCK
	}

	_, err := C.llapi_hsm_copytool_register(&cdt.hcp, C.CString(string(path)), 0, nil, flags)
	if err != nil {
		return nil, err
	}
	return &cdt, nil
}

// Recv blocks and waits for new action items from the coordinator.
// Retuns a slice of ActionItems.
func (cdt *Coordinator) Recv() ([]ActionItem, error) {
	var hal *C.struct_hsm_action_list
	var hai *C.struct_hsm_action_item
	var msgsize C.int

	if cdt.hcp == nil {
		return nil, errors.New("coordinator closed")
	}

	rc, err := C.llapi_hsm_copytool_recv(cdt.hcp, &hal, &msgsize)
	if rc < 0 || err != nil {
		return nil, err
	}

	items := make([]ActionItem, 0, int(hal.hal_count))
	hai = C.hai_first(hal)
	for i := 0; i < int(hal.hal_count); i++ {
		item := ActionItem{
			halFlags:  uint64(hal.hal_flags),
			archiveID: uint(hal.hal_archive_id),
			cdt:       cdt,
			hai:       *hai,
		}
		items = append(items, item)
		hai = C.hai_next(hai)
	}
	return items, nil
}

//GetFd returns copytool file descriptor
func (cdt *Coordinator) GetFd() int {
	return int(C.llapi_hsm_copytool_get_fd(cdt.hcp))
}

// Close terminates connection with coordinator.
func (cdt *Coordinator) Close() {
	if cdt.hcp != nil {
		glog.Info("closing coordinator.")
		C.llapi_hsm_copytool_unregister(&cdt.hcp)
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
		Action() HsmAction
		Fid() Fid
		DataFid() (Fid, error)
		Fd() (uintptr, error)
		Offset() uint64
		ArchiveID() uint
		Length() uint64
		String() string
	}
)

// Begin prepares an ActionItem for processing.
//
// returns an ActionItemHandle. The End method must be called to complete
// this action.
func (ai *ActionItem) Begin(openFlags int, isError bool) (ActionHandle, error) {
	mdtIndex := -1
	if ai.Action() == RESTORE && !isError {
		var err error
		mdtIndex, err = GetMdt(ai.cdt.root, ai.Fid())
		if err != nil {

			//Fixme...
			glog.Fatal(err)
		}
	}

	rc, err := C.llapi_hsm_action_begin(
		&ai.hcap,
		ai.cdt.hcp,
		&ai.hai,
		C.int(mdtIndex),
		C.int(openFlags),
		C.bool(isError))
	if rc < 0 {
		var extent *C.struct_hsm_extent
		C.llapi_hsm_action_end(&ai.hcap, extent, 0, C.int(-1))
		if err != nil {
			glog.Errorf("action_begin failed: %v\n", err)
			return nil, err
		}
		return nil, IoError("IO error")

	}
	return (*ActionItemHandle)(ai), nil
}

func (ai *ActionItem) String() string {
	return (*ActionItemHandle)(ai).String()
}

// ArchiveID returns the archive id associated with teh ActionItem.
func (ai *ActionItem) ArchiveID() uint {
	return ai.archiveID
}

// Action returns name of the action.
func (ai *ActionItem) Action() HsmAction {
	return HsmAction(ai.hai.hai_action)
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *ActionItem) Fid() Fid {
	return NewFid(&ai.hai.hai_fid)
}

// FailImmediately completes the ActinoItem with given error.
// The passed ActionItem is no longer valid when this function returns.
func (ai *ActionItem) FailImmediately(errval int) {
	aih, err := ai.Begin(0, true)
	if err != nil {
		glog.Errorf("begin failed: %s", ai.String())
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

func (ai *ActionItemHandle) String() string {
	return fmt.Sprintf("AI: %x %v %v %d,%v", ai.hai.hai_cookie, ai.Action(), ai.Fid(), ai.Offset(), lengthStr(ai.Length()))
}

// Progress reports current progress of an action.
func (ai *ActionItemHandle) Progress(offset uint64, length uint64, totalLength uint64, flags int) error {
	extent := C.struct_hsm_extent{C.__u64(offset), C.__u64(length)}
	rc, err := C.llapi_hsm_action_progress(ai.hcap, &extent, C.__u64(totalLength), C.int(flags))
	if rc < 0 || err != nil {
		return err
	}
	return nil
}

// End completes an action with specified status.
// No more requests should be made on this action after calling this.
func (ai *ActionItemHandle) End(offset uint64, length uint64, flags int, errval int) error {
	extent := C.struct_hsm_extent{C.__u64(offset), C.__u64(length)}
	rc, err := C.llapi_hsm_action_end(&ai.hcap, &extent, C.int(flags), C.int(errval))
	if rc < 0 || err != nil {
		return err
	}
	return nil
}

// HsmAction indentifies which action to perform.
type HsmAction uint32

// HSM Action constants
const (
	NONE    = HsmAction(C.HSMA_NONE)
	ARCHIVE = HsmAction(C.HSMA_ARCHIVE)
	RESTORE = HsmAction(C.HSMA_RESTORE)
	REMOVE  = HsmAction(C.HSMA_REMOVE)
	CANCEL  = HsmAction(C.HSMA_CANCEL)
)

func (action HsmAction) String() string {
	return C.GoString(C.hsm_copytool_action2name(C.enum_hsm_copytool_action(action)))
	// i kinda prefer lowercase...
	// switch action {
	// case NONE:
	// 	return "noop"
	// case ARCHIVE:
	// 	return "archive"
	// case RESTORE:
	// 	return "restore"
	// case REMOVE:
	// 	return "remove"
	// case CANCEL:
	// 	return "cancel"
	// }
}

// Action returns name of the action.
func (ai *ActionItemHandle) Action() HsmAction {
	return HsmAction(ai.hai.hai_action)
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *ActionItemHandle) Fid() Fid {
	return NewFid(&ai.hai.hai_fid)
}

// DataFid returns the FID of the data file.
// This file should be used for all Lustre IO for archive and restore commands.
func (ai *ActionItemHandle) DataFid() (Fid, error) {
	var cfid C.lustre_fid
	rc, err := C.llapi_hsm_action_get_dfid(ai.hcap, &cfid)
	if rc < 0 || err != nil {
		return nil, err
	}
	return NewFid(&cfid), nil
}

// Fd returns the file descriptor of the DataFid.
// If used, this Fd must be closed prior to calling End.
func (ai *ActionItemHandle) Fd() (uintptr, error) {
	rc, err := C.llapi_hsm_action_get_fd(ai.hcap)
	if rc < 0 || err != nil {
		return 0, err
	}
	return uintptr(rc), nil
}

// Offset returns the offset for the action.
func (ai *ActionItemHandle) Offset() uint64 {
	return uint64(ai.hai.hai_extent.offset)
}

// Length returns the length of the action request.
func (ai *ActionItemHandle) Length() uint64 {
	return uint64(ai.hai.hai_extent.length)
}

// ArchiveID returns archive for this action.
// Duplicating this on the action allows actions to be
// self-contained.
func (ai *ActionItemHandle) ArchiveID() uint {
	return ai.archiveID
}

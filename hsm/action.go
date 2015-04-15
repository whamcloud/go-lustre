package hsm

import (
	"errors"
	"fmt"

	"github.com/golang/glog"
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/llapi"
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
	ActionItem struct {
		cdt       *Coordinator
		hcap      *llapi.HsmCopyActionPrivate
		hai       llapi.HsmActionItem
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
// Retuns a slice of ActionItems.
func (cdt *Coordinator) Recv() ([]ActionItem, error) {

	if cdt.hcp == nil {
		return nil, errors.New("coordinator closed")
	}
	actionList, err := llapi.HsmCopytoolRecv(cdt.hcp)
	if err != nil {
		return nil, err
	}
	items := make([]ActionItem, len(actionList.Items))
	for i, hai := range actionList.Items {
		item := ActionItem{
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
		glog.Info("closing coordinator.")
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
		mdtIndex, err = fs.GetMdt(ai.cdt.root, ai.Fid())
		if err != nil {

			//Fixme...
			glog.Fatal(err)
		}
	}
	var err error
	ai.hcap, err = llapi.HsmActionBegin(
		ai.cdt.hcp,
		&ai.hai,
		mdtIndex,
		openFlags,
		isError)
	if err != nil {
		llapi.HsmActionEnd(&ai.hcap, 0, 0, 0, -1)
		glog.Errorf("action_begin failed: %v\n", err)
		return nil, err

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
func (ai *ActionItem) Action() llapi.HsmAction {
	return ai.hai.Action
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *ActionItem) Fid() *lustre.Fid {
	return ai.hai.Fid
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
	return fmt.Sprintf("AI: %x %v %v %d,%v", ai.hai.Cookie, ai.Action(), ai.Fid(), ai.Offset(), lengthStr(ai.Length()))
}

// Progress reports current progress of an action.
func (ai *ActionItemHandle) Progress(offset uint64, length uint64, totalLength uint64, flags int) error {
	return llapi.HsmActionProgress(ai.hcap, offset, length, totalLength, flags)
}

// End completes an action with specified status.
// No more requests should be made on this action after calling this.
func (ai *ActionItemHandle) End(offset uint64, length uint64, flags int, errval int) error {
	return llapi.HsmActionEnd(&ai.hcap, offset, length, flags, errval)
}

// Action returns name of the action.
func (ai *ActionItemHandle) Action() llapi.HsmAction {
	return ai.hai.Action
}

// Fid returns the FID for the actual file for ths action.
// This fid or xattrs on this file can be used as a key with
// the HSM backend.
func (ai *ActionItemHandle) Fid() *lustre.Fid {
	return ai.hai.Fid
}

// Cookie returns the action identifier.
func (ai *ActionItemHandle) Cookie() uint64 {
	return ai.hai.Cookie
}

// DataFid returns the FID of the data file.
// This file should be used for all Lustre IO for archive and restore commands.
func (ai *ActionItemHandle) DataFid() (*lustre.Fid, error) {
	return llapi.HsmActionGetDataFid(ai.hcap)
}

// Fd returns the file descriptor of the DataFid.
// If used, this Fd must be closed prior to calling End.
func (ai *ActionItemHandle) Fd() (uintptr, error) {
	fd, err := llapi.HsmActionGetFd(ai.hcap)
	if err != nil {
		return 0, err
	}
	return fd, nil
}

// Offset returns the offset for the action.
func (ai *ActionItemHandle) Offset() uint64 {
	return uint64(ai.hai.Extent.Offset)
}

// Length returns the length of the action request.
func (ai *ActionItemHandle) Length() uint64 {
	return uint64(ai.hai.Extent.Length)
}

// ArchiveID returns archive for this action.
// Duplicating this on the action allows actions to be
// self-contained.
func (ai *ActionItemHandle) ArchiveID() uint {
	return ai.archiveID
}

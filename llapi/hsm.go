package llapi

//
// #cgo LDFLAGS: -llustreapi
// #include <fcntl.h>
// #include <lustre/lustreapi.h>
//
// /* typecast hal_fsname to char *  */
// char * hal_fsname(struct hsm_action_list *hal) {
//    return (char *) hal->hal_fsname;
// }
import "C"
import (
	"errors"
	"unsafe"

	"github.intel.com/hpdd/lustre"
)

type HsmCopytoolPrivate C.struct_hsm_copytool_private
type HsmCopyActionPrivate C.struct_hsm_copyaction_private

type (
	HsmActionList struct {
		Version    uint32
		CompoundID uint64
		Flags      uint64
		ArchiveID  uint
		FsName     string
		Items      []HsmActionItem
	}
	HsmActionItem struct {
		Action HsmAction
		Fid    *lustre.Fid
		Extent *HsmExtent
		Cookie uint64
		Data   []byte
		hai    C.struct_hsm_action_item
	}
	HsmExtent struct {
		Offset uint64
		Length uint64
	}
)

// HsmAction indentifies which action to perform.
type HsmAction uint32

// HSM Action constants
const (
	HsmActionNone    = HsmAction(C.HSMA_NONE)
	HsmActionArchive = HsmAction(C.HSMA_ARCHIVE)
	HsmActionRestore = HsmAction(C.HSMA_RESTORE)
	HsmActionRemove  = HsmAction(C.HSMA_REMOVE)
	HsmActionCancel  = HsmAction(C.HSMA_CANCEL)
)

func (action HsmAction) String() string {
	return C.GoString(C.hsm_copytool_action2name(C.enum_hsm_copytool_action(action)))
}

// HsmCopytooLFlags for initializing copytool
type HsmCopytoolFlags int

const (
	// CopytooDefault set of flags (currently none)
	CopytoolDefault = HsmCopytoolFlags(0)
	// CopytoolNonBlock disables blocking so the poll can be used on the copytoold file descriptor.
	CopytoolNonBlock = HsmCopytoolFlags(C.O_NONBLOCK)
)

// HsmCopytooLRegister connects the agent to the HSM Coordinators on all the MDTs.
// if CopytooLNonBLock flag is passed, then the HsmCopytoolRecv() will not block
// and poll() could used on the coordinator's descriptor.
func HsmCopytoolRegister(path string, archiveCount int, archives []int, flags HsmCopytoolFlags) (*HsmCopytoolPrivate, error) {
	var hcp *C.struct_hsm_copytool_private
	rc, err := C.llapi_hsm_copytool_register(&hcp, C.CString(string(path)), 0, nil, C.int(flags))
	if err := isError(rc, err); err != nil {
		return nil, err
	}
	return (*HsmCopytoolPrivate)(hcp), nil

}

// HsmCopytooGetFd returns the descriptor for the coordinator. Useful when non-blocking
// mode is being used.
func HsmCopytoolGetFd(hcp *HsmCopytoolPrivate) int {
	return int(C.llapi_hsm_copytool_get_fd((*C.struct_hsm_copytool_private)(hcp)))
}

// HsmCopytooLUnregister closes the connection to the coordinator.
func HsmCopytoolUnregister(hcp **HsmCopytoolPrivate) {
	h := (*C.struct_hsm_copytool_private)(*hcp)
	*hcp = nil
	C.llapi_hsm_copytool_unregister(&h)
}

// HsmCopytooLRecv retuns a list of actions received from the coordinator.
func HsmCopytoolRecv(hcp *HsmCopytoolPrivate) (*HsmActionList, error) {
	var hal *C.struct_hsm_action_list
	var hai *C.struct_hsm_action_item
	var msgsize C.int

	if hcp == nil {
		return nil, errors.New("coordinator closed")
	}

	rc, err := C.llapi_hsm_copytool_recv((*C.struct_hsm_copytool_private)(hcp), &hal, &msgsize)
	if err := isError(rc, err); err != nil {
		return nil, err
	}
	var actionList HsmActionList
	actionList.Items = make([]HsmActionItem, 0, int(hal.hal_count))
	actionList.ArchiveID = uint(hal.hal_archive_id)
	actionList.Flags = uint64(hal.hal_flags)
	actionList.Version = uint32(hal.hal_version)
	actionList.CompoundID = uint64(hal.hal_compound_id)
	actionList.FsName = C.GoString(C.hal_fsname(hal))

	hai = C.hai_first(hal)
	for i := 0; i < int(hal.hal_count); i++ {
		item := HsmActionItem{
			hai:    *hai,
			Action: HsmAction(hai.hai_action),
			Fid:    fromCFid(&hai.hai_fid),
			Extent: &HsmExtent{
				Offset: uint64(hai.hai_extent.offset),
				Length: uint64(hai.hai_extent.length),
			},
			Cookie: uint64(hai.hai_cookie),
			Data:   fetchData(hai),
		}
		actionList.Items = append(actionList.Items, item)
		hai = C.hai_next(hai)
	}
	return &actionList, nil
}

func fetchData(hai *C.struct_hsm_action_item) []byte {
	len := int(hai.hai_len) - int(unsafe.Sizeof(*hai))
	return C.GoBytes(unsafe.Pointer(&hai.hai_data), C.int(len))
}

// HsmActionBegin initializes the action so it can be processed by the copytool.
func HsmActionBegin(hcp *HsmCopytoolPrivate, hai *HsmActionItem, mdtIndex int, openFlags int, isErr bool) (*HsmCopyActionPrivate, error) {
	var hcap *C.struct_hsm_copyaction_private
	rc, err := C.llapi_hsm_action_begin(
		&hcap,
		(*C.struct_hsm_copytool_private)(hcp),
		&hai.hai,
		C.int(mdtIndex),
		C.int(openFlags),
		C.bool(isErr))
	if err := isError(rc, err); err != nil {
		return nil, err
	}
	return (*HsmCopyActionPrivate)(hcap), nil
}

// HsmActionProgress informs the coordinator of how much progress has been made to
// file. This also serves as heartbeat, and if the CDT hasn't received progress
// on the file before the timeout window ends (defaults to 1 hour), the the action
// will be cancelled and reassigned to a different agent. (TODO: confirm actual CDT
// behavior on timeout)
func HsmActionProgress(hcap *HsmCopyActionPrivate, offset, length, totalLength uint64, flags int) error {
	extent := C.struct_hsm_extent{
		offset: C.__u64(offset),
		length: C.__u64(length),
	}
	rc, err := C.llapi_hsm_action_progress((*C.struct_hsm_copyaction_private)(hcap), &extent, C.__u64(totalLength), C.int(flags))
	if err := isError(rc, err); err != nil {
		return err
	}
	return nil

}

// HsmActionEnd ends the HSM transaction. If this is was a successful restore, then the
// the layout of the temporary data file is swapped with the actual file, and the group
// lock is dropped so applications can read the file. Ensure all data being written to the
// data file has been flushed before call End.
func HsmActionEnd(hcap **HsmCopyActionPrivate, offset, length uint64, flags, errVal int) error {
	h := (*C.struct_hsm_copyaction_private)(*hcap)
	*hcap = nil
	extent := C.struct_hsm_extent{
		offset: C.__u64(offset),
		length: C.__u64(length),
	}
	rc, err := C.llapi_hsm_action_end(
		&h,
		&extent,
		C.int(flags),
		C.int(errVal))
	if err := isError(rc, err); err != nil {
		return err
	}
	return nil
}

// HsmActionGetDataFid returns the fid that shoudl be used to restore data to.
// It can also be used to copy data from for archiving.
func HsmActionGetDataFid(hcap *HsmCopyActionPrivate) (*lustre.Fid, error) {
	var cfid C.lustre_fid
	rc, err := C.llapi_hsm_action_get_dfid((*C.struct_hsm_copyaction_private)(hcap), &cfid)
	if err := isError(rc, err); err != nil {
		return nil, err
	}
	return fromCFid(&cfid), nil
}

// HsmActonGetFd returns filedescriptor of the data fid. The data fid
// can also be opened directly, so this isn't strictly necessary.
func HsmActionGetFd(hcap *HsmCopyActionPrivate) (uintptr, error) {
	rc, err := C.llapi_hsm_action_get_fd((*C.struct_hsm_copyaction_private)(hcap))
	if err := isError(rc, err); err != nil {
		return 0, err
	}
	return uintptr(rc), err
}

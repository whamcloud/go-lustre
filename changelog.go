package lustre

// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
//
// /* cr_tfid in a union, so cgo essentially ignores it */
// lustre_fid changelog_rec_tfid(struct changelog_rec *rec) {
//    return rec->cr_tfid;
// }
//
import "C"

import (
	"bytes"
	"fmt"
	"strings"
	"time"
	"unsafe"
)

type (
	// Changelog is a handle for an open changelog.
	Changelog struct {
		priv *byte
	}

	// ChangelogEntry is a change log entry. JobId is only avaiable if HasJob() is true.
	// The Source* fields are only available if HasRename() is true.
	ChangelogEntry struct {
		Name            string
		Flags           uint
		Index           int64
		Prev            uint
		Time            time.Time
		Type            uint
		TypeName        string
		TargetFid       Fid
		ParentFid       Fid
		SourceName      string
		SourceFid       Fid
		SourceParentFid Fid
		JobID           string
	}
)

// Changelog Types
const (
	CL_MARK     = 0
	CL_CREATE   = 1  /* namespace */
	CL_MKDIR    = 2  /* namespace */
	CL_HARDLINK = 3  /* namespace */
	CL_SOFTLINK = 4  /* namespace */
	CL_MKNOD    = 5  /* namespace */
	CL_UNLINK   = 6  /* namespace */
	CL_RMDIR    = 7  /* namespace */
	CL_RENAME   = 8  /* namespace */
	CL_EXT      = 9  /* namespace extended record (2nd half of rename) */
	CL_OPEN     = 10 /* not currently used */
	CL_CLOSE    = 11 /* may be written to log only with mtime change */
	CL_LAYOUT   = 12 /* file layout/striping modified */
	CL_TRUNC    = 13
	CL_SETATTR  = 14
	CL_XATTR    = 15
	CL_HSM      = 16 /* HSM specific events, see flags */
	CL_MTIME    = 17 /* Precedence: setattr > mtime > ctime > atime */
	CL_CTIME    = 18
	CL_ATIME    = 19
	CL_LAST
)

// Changelog Flags
const (
	// Anything under the flagmask may be per-type (if desired)
	CLF_FLAGMASK = C.CLF_FLAGMASK

	// Flags for unlink
	CLF_UNLINK_LAST       = C.CLF_UNLINK_LAST       // Unlink of last hardlink
	CLF_UNLINK_HSM_EXISTS = C.CLF_UNLINK_HSM_EXISTS // Unlink of last link, HSM archive may exist

	// Flags for rename
	CLF_RENAME_LAST        = C.CLF_RENAME_LAST        // rename unlink last hardlink of target
	CLF_RENAME_LAST_EXISTS = C.CLF_RENAME_LAST_EXISTS // rename unlink last hardlink of target, HSM archive may exist

)

type HsmEvent int32

// HSM Event Types
const (
	HE_ARCHIVE HsmEvent = C.HE_ARCHIVE
	HE_RESTORE HsmEvent = C.HE_RESTORE
	HE_CANCEL  HsmEvent = C.HE_CANCEL
	HE_RELEASE HsmEvent = C.HE_RELEASE
	HE_REMOVE  HsmEvent = C.HE_REMOVE
	HE_STATE   HsmEvent = C.HE_STATE
	HE_SPARE1  HsmEvent = C.HE_SPARE1
	HE_SPARE2  HsmEvent = C.HE_SPARE2
)

func (he *HsmEvent) String() string {
	switch *he {
	case HE_ARCHIVE:
		return "Archive"
	case HE_RESTORE:
		return "Restore"
	case HE_CANCEL:
		return "Cancel"
	case HE_RELEASE:
		return "Release"
	case HE_REMOVE:
		return "Remove"
	case HE_STATE:
		return "Changed State"
	case HE_SPARE1:
		return "Spare1"
	case HE_SPARE2:
		return "Spare2"
	default:
		panic(fmt.Sprintf("Unknown HsmEvent: %d", he))
	}
}

// ChangelogOpen returns an object that can be used to read changelog entries.
func ChangelogOpen(path string, follow bool, startRec int64) *Changelog {
	cl := Changelog{}
	var flags = C.CHANGELOG_FLAG_BLOCK | C.CHANGELOG_FLAG_JOBID
	if follow {
		flags |= C.CHANGELOG_FLAG_FOLLOW
	}

	rc, err := C.llapi_changelog_start((*unsafe.Pointer)(unsafe.Pointer(&cl.priv)),
		uint32(flags),
		C.CString(path),
		C.longlong(startRec))
	if rc != 0 {
		fmt.Printf("error %v, %v", rc, err)
		return nil
	}
	return &cl
}

// Close the changelog handle.
func (cl *Changelog) Close() {
	_, err := C.llapi_changelog_fini((*unsafe.Pointer)(unsafe.Pointer(&cl.priv)))
	cl.priv = nil
	if err != nil {
		fmt.Println(err)
	}
}

// FlagStrings convers the log entry flogs into human readable format.
func (entry *ChangelogEntry) FlagStrings() []string {
	var flagStrings []string

	switch entry.Type {
	case C.CL_HSM:
		event := HsmEvent(C.hsm_get_cl_event(C.__u16(entry.Flags)))
		flagStrings = append(flagStrings, event.String())
		hsmFlags := C.hsm_get_cl_flags(C.int(entry.Flags))
		switch hsmFlags {
		case C.CLF_HSM_DIRTY:
			flagStrings = append(flagStrings, "Dirty")
		}
	case C.CL_UNLINK:
		if entry.Flags&C.CLF_UNLINK_LAST > 0 {
			flagStrings = append(flagStrings, "Last Hardlink")
		}
		if entry.Flags&C.CLF_UNLINK_HSM_EXISTS > 0 {
			flagStrings = append(flagStrings, "HSM Cruft")
		}
	case C.CL_RENAME:
		if entry.Flags&C.CLF_RENAME_LAST > 0 {
			flagStrings = append(flagStrings, "Last Hardlink")
		}
		if entry.Flags&C.CLF_RENAME_LAST_EXISTS > 0 {
			flagStrings = append(flagStrings, "HSM Cruft")
		}
	}

	return flagStrings
}

// HasJob returns true if the entry has a JobID.
func (entry *ChangelogEntry) HasJob() bool {
	return entry.Flags&C.CLF_JOBID == C.CLF_JOBID
}

// HasRename returns true if entry rename info
func (entry *ChangelogEntry) HasRename() bool {
	return entry.Flags&C.CLF_RENAME == C.CLF_RENAME
}

func (entry *ChangelogEntry) String() string {
	var buffer bytes.Buffer
	s := C.GoString(C.changelog_type2str(C.int(entry.Type)))

	buffer.WriteString(fmt.Sprintf("%d ", entry.Index))
	buffer.WriteString(fmt.Sprintf("%02d%s ", entry.Type, s))
	buffer.WriteString(fmt.Sprintf("%s ", entry.Time))
	buffer.WriteString(fmt.Sprintf("%#x ", entry.Flags&C.CLF_FLAGMASK))
	buffer.WriteString(fmt.Sprintf("%s ", strings.Join(entry.FlagStrings(), ",")))
	if entry.HasJob() && len(entry.JobID) > 0 {
		buffer.WriteString(fmt.Sprintf("job=%s ", entry.JobID))
	}
	if entry.HasRename() && !entry.SourceFid.IsZero() {
		buffer.WriteString(fmt.Sprintf("%v/%v", entry.SourceParentFid, entry.SourceFid))
		if entry.SourceParentFid != entry.ParentFid {
			buffer.WriteString(fmt.Sprintf("->%v/%v ", entry.ParentFid, entry.TargetFid))
		} else {
			buffer.WriteString(" ")
		}
	} else {
		buffer.WriteString(fmt.Sprintf("%v/%v ", entry.ParentFid, entry.TargetFid))
	}
	if entry.HasRename() && len(entry.SourceName) > 0 {
		buffer.WriteString(fmt.Sprintf("%s->", entry.SourceName))
	}
	if len(entry.Name) > 0 {
		buffer.WriteString(entry.Name)
	}
	return buffer.String()
}

// GetNextLogEntry returns the next available log entry
// in the Changelog. This may block, depending on flags
// passed to ChangelogStart.
func (cl *Changelog) GetNextLogEntry() *ChangelogEntry {
	var rec *C.struct_changelog_rec

	rc := C.llapi_changelog_recv((unsafe.Pointer(cl.priv)),
		&rec)
	if rc != 0 {
		return nil
	}
	entry := ChangelogEntry{}

	entry.Index = int64(rec.cr_index)
	entry.Type = uint(rec.cr_type)
	entry.TypeName = C.GoString(C.changelog_type2str(C.int(entry.Type)))
	entry.Flags = uint(rec.cr_flags)
	entry.Prev = uint(rec.cr_prev)
	entry.Time = time.Unix(int64(rec.cr_time>>30), 0) // WTF?
	tfid := C.changelog_rec_tfid(rec)
	entry.TargetFid = NewFid(&tfid)
	entry.ParentFid = NewFid(&rec.cr_pfid)
	entry.Name = C.GoString(C.changelog_rec_name(rec))
	if entry.HasRename() {
		rename := C.changelog_rec_rename(rec)
		entry.SourceName = C.GoString(C.changelog_rec_sname(rec))
		entry.SourceFid = NewFid(&rename.cr_sfid)
		entry.SourceParentFid = NewFid(&rename.cr_spfid)
	}
	if entry.HasJob() {
		jobid := C.changelog_rec_jobid(rec)
		entry.JobID = C.GoString(&jobid.cr_jobid[0])
	}

	C.llapi_changelog_free(&rec)

	return &entry
}

// ChangelogClear delete records in changelog up to endRec.
func ChangelogClear(path string, idStr string, endRec int64) error {
	rc, err := C.llapi_changelog_clear(C.CString(path), C.CString(idStr), C.longlong(endRec))
	if rc < 0 || err != nil {
		return fmt.Errorf("changelog: Unable to clear log (%v, %v, %v): %d %v", path, idStr, endRec, rc, err)
	}
	return nil
}

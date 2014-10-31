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
	"time"
	"unsafe"
)

type (
	Changelog struct {
		priv *byte
	}

	ChangelogEntry struct {
		Name            string
		SourceName      string
		Flags           uint
		Index           int64
		Prev            uint
		Time            time.Time
		Type            uint
		TypeName        string
		TargetFid       Fid
		ParentFid       Fid
		SourceFid       Fid
		SourceParentFid Fid
		JobId           string
	}
)

// ChangelogOpen returns an object that can be used to read changelog entries.
func ChangelogOpen(path string, follow bool, startRec int64) *Changelog {
	clog := Changelog{}
	var flags = C.CHANGELOG_FLAG_BLOCK | C.CHANGELOG_FLAG_JOBID
	if follow {
		flags |= C.CHANGELOG_FLAG_FOLLOW
	}

	rc, err := C.llapi_changelog_start((*unsafe.Pointer)(unsafe.Pointer(&clog.priv)),
		uint32(flags),
		C.CString(path),
		C.longlong(startRec))
	if rc != 0 {
		fmt.Printf("error %v, %v", rc, err)
		return nil
	}
	return &clog
}

func (chglog *Changelog) Close() {
	_, err := C.llapi_changelog_fini((*unsafe.Pointer)(unsafe.Pointer(&chglog.priv)))
	chglog.priv = nil
	if err != nil {
		fmt.Println(err)
	}
}

func (entry *ChangelogEntry) HasJobExt() bool {
	return entry.Flags&C.CLF_JOBID == C.CLF_JOBID
}

func (entry *ChangelogEntry) HasRenameExt() bool {
	return entry.Flags&C.CLF_RENAME == C.CLF_RENAME
}

func (entry *ChangelogEntry) String() string {
	var buffer bytes.Buffer
	type_str := C.GoString(C.changelog_type2str(C.int(entry.Type)))

	buffer.WriteString(fmt.Sprintf("%d ", entry.Index))
	buffer.WriteString(fmt.Sprintf("%02d%s ", entry.Type, type_str))
	buffer.WriteString(fmt.Sprintf("%s ", entry.Time))
	buffer.WriteString(fmt.Sprintf("%#x ", entry.Flags&C.CLF_FLAGMASK))
	if entry.HasJobExt() && len(entry.JobId) > 0 {
		buffer.WriteString(fmt.Sprintf("job=%s ", entry.JobId))
	}
	if entry.HasRenameExt() && !entry.SourceFid.IsZero() {
		buffer.WriteString(fmt.Sprintf("%v/%v", entry.SourceParentFid, entry.SourceFid))
		if entry.SourceParentFid != entry.ParentFid {
			buffer.WriteString(fmt.Sprintf("->%v/%v ", entry.ParentFid, entry.TargetFid))
		} else {
			buffer.WriteString(" ")
		}
	} else {
		buffer.WriteString(fmt.Sprintf("%v/%v ", entry.ParentFid, entry.TargetFid))
	}
	if entry.HasRenameExt() && len(entry.SourceName) > 0 {
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
func (clog *Changelog) GetNextLogEntry() *ChangelogEntry {
	var rec *C.struct_changelog_rec
	var rec_rename_ext *C.struct_changelog_ext_rename
	var rec_jobid_ext *C.struct_changelog_ext_jobid

	rc := C.llapi_changelog_recv((unsafe.Pointer(clog.priv)),
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
	entry.TargetFid = Fid(C.changelog_rec_tfid(rec))
	entry.ParentFid = Fid(rec.cr_pfid)
	entry.Name = C.GoString(C.changelog_rec_name(rec))
	if entry.HasRenameExt() {
		rec_rename_ext = C.changelog_rec_rename(rec)
		if !Fid(rec_rename_ext.cr_sfid).IsZero() {
			entry.SourceName = C.GoString(C.changelog_rec_sname(rec))
			entry.SourceFid = Fid(rec_rename_ext.cr_sfid)
			entry.SourceParentFid = Fid(rec_rename_ext.cr_spfid)
		}
	}
	if entry.HasJobExt() {
		rec_jobid_ext = C.changelog_rec_jobid(rec)
		entry.JobId = C.GoString(&rec_jobid_ext.cr_jobid[0])
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

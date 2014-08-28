package lustre

// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
//
// /* Not sure why cgo didn't like this macro. */
// int is_extended(struct changelog_ext_rec *rec) {
//     return CHANGELOG_REC_EXTENDED(rec);
// }
//
// /* cr_tfid in a union, so cgo essentially ignores it */
// lustre_fid changelog_rec_tfid(struct changelog_ext_rec *rec) {
//    return rec->cr_tfid;
// }
//
import "C"

import (
	"fmt"
	"strings"
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
		IsExtended      bool
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
	}
)

// ChangelogOpen returns an object that can be used to read changelog entries.
func ChangelogOpen(path string, follow bool, startRec int64) *Changelog {
	clog := Changelog{}
	var flags = C.CHANGELOG_FLAG_BLOCK
	if follow {
		flags |= C.CHANGELOG_FLAG_FOLLOW
	}

	rc, err := C.llapi_changelog_start((*unsafe.Pointer)(unsafe.Pointer(&clog.priv)),
		C.int(flags),
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

func (entry *ChangelogEntry) String() string {
	output := make([]string, 0)
	type_str := C.GoString(C.changelog_type2str(C.int(entry.Type)))

	output = append(output, fmt.Sprintf("%04d", entry.Index))
	output = append(output, type_str)
	output = append(output, fmt.Sprint(entry.Time))
	output = append(output, fmt.Sprintf("0x%X", entry.Flags&C.CLF_FLAGMASK))
	if entry.IsExtended {
		output = append(output, fmt.Sprintf("%v/%v", entry.SourceParentFid, entry.SourceFid))
		if entry.SourceParentFid != entry.ParentFid {
			output = append(output, "->", fmt.Sprintf("%v/%v", entry.ParentFid, entry.TargetFid))
		}
	} else {
		output = append(output, fmt.Sprintf("%v/%v", entry.ParentFid, entry.TargetFid))
	}
	if entry.IsExtended {
		output = append(output, entry.SourceName, "->")
	}
	if len(entry.Name) > 0 {
		output = append(output, entry.Name)
	}
	return strings.Join(output, " ")
}

// GetNextLogEntry returns the next available log entry
// in the Changelog. This may block, depending on flags
// passed to ChangelogStart.
func (clog *Changelog) GetNextLogEntry() *ChangelogEntry {
	var rec *C.struct_changelog_ext_rec
	rc := C.llapi_changelog_recv((unsafe.Pointer(clog.priv)),
		&rec)
	if rc != 0 {
		return nil
	}
	entry := ChangelogEntry{}

	entry.IsExtended = bool(C.is_extended(rec) == 1)
	entry.Index = int64(rec.cr_index)
	entry.Type = uint(rec.cr_type)
	entry.TypeName = C.GoString(C.changelog_type2str(C.int(entry.Type)))
	entry.Flags = uint(rec.cr_flags) & C.CLF_FLAGMASK
	entry.Prev = uint(rec.cr_prev)
	entry.Time = time.Unix(int64(rec.cr_time>>30), 0) // WTF?
	entry.TargetFid = Fid(C.changelog_rec_tfid(rec))
	entry.ParentFid = Fid(rec.cr_pfid)
	entry.Name = C.GoStringN((*C.char)(unsafe.Pointer(&rec.cr_name)),
		C.int(C.strlen((*C.char)(unsafe.Pointer(&rec.cr_name)))))
	if entry.IsExtended && !Fid(rec.cr_sfid).IsZero() {
		entry.SourceName = C.GoStringN(C.changelog_rec_sname(rec),
			C.changelog_rec_snamelen(rec))
		entry.SourceFid = Fid(rec.cr_sfid)
		entry.SourceParentFid = Fid(rec.cr_spfid)
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

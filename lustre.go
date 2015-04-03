package lustre

import "time"

type (
	// ChangelogRecord represents a Lustre Changelog record
	ChangelogRecord interface {
		Index() int64
		Name() string
		Type() string
		TypeCode() uint
		Time() time.Time
		TargetFid() *Fid
		ParentFid() *Fid
		SourceName() string
		SourceFid() *Fid
		SourceParentFid() *Fid
		IsRename() bool
		IsLastRename() (bool, bool)
		IsLastUnlink() (bool, bool)
		JobID() string
		String() string
	}

	// ChangelogHandle represents an interface to a Lustre Changelog
	ChangelogHandle interface {
		Open(bool) error
		OpenAt(int64, bool) error
		Close() error
		NextRecord() (ChangelogRecord, error)
		Clear(string, int64) error
		String() string
	}

	// ChangelogRecordIterator iterates over Records
	ChangelogRecordIterator interface {
		NextRecord() (ChangelogRecord, error)
	}
)

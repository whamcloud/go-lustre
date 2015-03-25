package changelog

import (
	"time"

	"github.intel.com/hpdd/lustre"
)

type (
	// Record represents a Lustre Changelog record
	Record interface {
		Index() int64
		Name() string
		Type() string
		Time() time.Time
		TargetFid() *lustre.Fid
		ParentFid() *lustre.Fid
		SourceName() string
		SourceFid() *lustre.Fid
		SourceParentFid() *lustre.Fid
		JobID() string
		String() string
	}

	// Handle represents an interface to a Lustre Changelog
	Handle interface {
		Open(bool) error
		OpenAt(int64, bool) error
		Close() error
		NextRecord() (Record, error)
		Clear(string, int64) error
		String() string
	}

	// RecordIterator iterates over Records
	RecordIterator interface {
		NextRecord() (Record, error)
	}
)

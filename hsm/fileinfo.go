package hsm

import (
	"bytes"
	"fmt"
	"strings"

	"github.intel.com/hpdd/lustre/llapi"
)

// FileStatus describes a file's current HSM status, including its
// associated archive ID (if any), and HSM state
type FileStatus struct {
	ArchiveID uint32
	state     llapi.HsmFileState
}

// Exists is true if an HSM archive action has been initiated for a file. A
// copy or partial copy of the file may exist in the backend. Or it might not.
func (f *FileStatus) Exists() bool {
	return f.state.HasFlag(llapi.HsmFileExists)
}

// Archived is true if a complete (but possibly stale) copy of the file
// contents are stored in the archive.
func (f *FileStatus) Archived() bool {
	return f.state.HasFlag(llapi.HsmFileArchived)
}

// Dirty means the file has been modified since the last time it was archived.
func (f *FileStatus) Dirty() bool {
	return f.state.HasFlag(llapi.HsmFileDirty)
}

// Released is true if the contents of the file have been removed from the
// filesystem. Only possible if the file has been Archived.
func (f *FileStatus) Released() bool {
	return f.state.HasFlag(llapi.HsmFileReleased)
}

// NoRelease prevents the file data from being relesed, even if it is Archived.
func (f *FileStatus) NoRelease() bool {
	return f.state.HasFlag(llapi.HsmFileNoRelease)
}

// NoArchive inhibits archiving the file. (Useful for temporary files perhaps.)
func (f *FileStatus) NoArchive() bool {
	return f.state.HasFlag(llapi.HsmFileNoArchive)
}

// Lost means the copy of the file in the archive is not accessible.
func (f *FileStatus) Lost() bool {
	return f.state.HasFlag(llapi.HsmFileLost)
}

// Flags returns a slice of HSM state flag strings
func (f *FileStatus) Flags() []string {
	return f.state.Flags()
}

func (f *FileStatus) String() string {
	return FileStatusString(f, true)
}

// GetFileStatus returns a *FileStatus for the given path
func GetFileStatus(filePath string) (*FileStatus, error) {
	s, id, err := llapi.GetHsmFileStatus(filePath)
	if err != nil {
		return nil, err
	}
	return &FileStatus{ArchiveID: id, state: s}, nil
}

func summarizeStatus(s *FileStatus) string {
	var buf bytes.Buffer

	if s.Exists() {
		switch {
		case s.Released():
			fmt.Fprintf(&buf, "released")
			if s.Lost() {
				fmt.Fprintf(&buf, "+lost")
			}
		case s.Lost():
			fmt.Fprintf(&buf, "lost")
		case s.Dirty():
			fmt.Fprintf(&buf, "dirty")
		case s.Archived():
			fmt.Fprintf(&buf, "archived")
		default:
			fmt.Fprintf(&buf, "unarchived")
		}
	} else {
		fmt.Fprintf(&buf, "-")
	}

	if s.NoRelease() {
		fmt.Fprintf(&buf, "@")
	}

	if s.NoArchive() {
		fmt.Fprintf(&buf, "%")
	}

	return buf.String()
}

// FileStatusString returns a string describing the given FileStatus
func FileStatusString(s *FileStatus, summarize bool) string {
	// NB: On the fence about whether or not this stuff belongs here --
	// it's arguably application-specific display logic, but it seems
	// like it'd be nice to not have to reinvent this wheel all the time.
	var buf bytes.Buffer

	if s.Exists() {
		fmt.Fprintf(&buf, "%d", s.ArchiveID)
	} else {
		fmt.Fprintf(&buf, "-")
	}

	if summarize {
		fmt.Fprintf(&buf, " %s", summarizeStatus(s))
	} else {
		if len(s.Flags()) > 0 {
			fmt.Fprintf(&buf, " %s", strings.Join(s.Flags(), ","))
		} else {
			fmt.Fprintf(&buf, " -")
		}
	}

	return buf.String()
}

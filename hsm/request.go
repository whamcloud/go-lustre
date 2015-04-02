package hsm

import (
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/llapi"
)

// RequestHsmArchive submits a request to the coordinator for the
// specified list of fids to be archived to the specfied archive id.
func RequestHsmArchive(fsID fs.ID, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(fsID, llapi.UserArchive, archiveID, fids)
}

// RequestHsmRestore submits a request to the coordinator for the
// specified list of fids to be restored from the specfied archive id.
func RequestHsmRestore(fsID fs.ID, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(fsID, llapi.UserRestore, archiveID, fids)
}

// RequestHsmRelease submits a request to the coordinator for the
// specified list of fids to be released.
func RequestHsmRelease(fsID fs.ID, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(fsID, llapi.UserRelease, archiveID, fids)
}

// RequestHsmRemove submits a request to the coordinator for the
// specified list of fids to be removed from the HSM backend.
func RequestHsmRemove(fsID fs.ID, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(fsID, llapi.UserRemove, archiveID, fids)
}

// RequestHsmCancel submits a request to the coordinator to cancel any
// outstanding requests involving the specified list of fids.
func RequestHsmCancel(fsID fs.ID, archiveID uint, fids []*lustre.Fid) error {
	return hsmRequest(fsID, llapi.UserCancel, archiveID, fids)
}

func hsmRequest(fsID fs.ID, cmd llapi.HsmUserAction, archiveID uint, fids []*lustre.Fid) error {
	mnt, err := fsID.Path()
	if err != nil {
		return err
	}

	if _, err = llapi.HsmRequest(mnt, cmd, archiveID, fids); err != nil {
		return err
	}
	return nil
}

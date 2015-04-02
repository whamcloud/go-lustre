package hsm

import (
	"os"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/llapi"
)

func Import(f string,
	archive uint,
	fi os.FileInfo,
	stripeSize uint64,
	stripeOffset int,
	stripeCount int,
	stripePattern int,
	poolName string) (*lustre.Fid, error) {
	return llapi.HsmImport(f, archive, fi, stripeSize, stripeOffset, stripeCount, stripePattern, poolName)
}

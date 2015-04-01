package hsm

import "github.intel.com/hpdd/lustre/llapi"

// State returns the HsmState for the given file or error if the
// file is not on a Lustre filesystem.
func State(file string) (*llapi.HsmState, error) {
	return llapi.HsmStateGet(file)
}

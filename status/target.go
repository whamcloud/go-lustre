package status

//
// #cgo LDFLAGS: -llustreapi
// #include <lustre/lustreapi.h>
//
import "C"

import (
	"bufio"
	"fmt"
	"os"
	"path"
)

type (
	// TargetIndex is the name of a target and its index.
	TargetIndex struct {
		Index int
		Name  string
	}
)

// LovName returns the uniqe name for the LOV devcie for the client associated with the path.
func LovName(p string) (string, error) {
	var obd C.struct_obd_uuid
	rc, err := C.llapi_file_get_lov_uuid(C.CString(p), &obd)
	if rc < 0 || err != nil {
		return "", err
	}
	s := C.GoString(&obd.uuid[0])
	return s, nil
}

// LmvName returns the uniqe name for the LMV device for the client associated with the path.
func LmvName(p string) (string, error) {
	var obd C.struct_obd_uuid
	rc, err := C.llapi_file_get_lmv_uuid(C.CString(p), &obd)
	if rc < 0 || err != nil {
		return "", err
	}
	s := C.GoString(&obd.uuid[0])
	return s, nil
}

// LovTargets returns uuids and indices of the targts in an LOV.
// Path refers to a file or directory in a Lustre filesystem.
func LovTargets(p string) (result []TargetIndex, err error) {
	lov, err := LovName(p)
	if err != nil {
		return nil, err
	}

	return getTargetIndex("lov", lov)
}

// LmvTargets returns uuids and indices of the targts in an LmV.
// Path refers to a file or directory in a Lustre filesystem.
func LmvTargets(p string) (result []TargetIndex, err error) {
	lmv, err := LmvName(p)
	if err != nil {
		return nil, err
	}

	return getTargetIndex("lmv", lmv)
}

func getTargetIndex(targetType, targetName string) (result []TargetIndex, err error) {
	name := path.Join(procBase, targetType, targetName, "target_obd")
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var tgt TargetIndex
		fmt.Sscanf(scanner.Text(), "%d: %s", &tgt.Index, &tgt.Name)
		result = append(result, tgt)
	}
	return result, nil
}

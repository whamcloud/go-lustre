package main

/*
#cgo LDFLAGS: -llustreapi
#include <stdlib.h>
#include <lustre/lustreapi.h>
#include <lustre/lustreapi.h>

*/
import "C"

import (
	"flag"
	"fmt"
	"log"
	"os"
	"unsafe"

	"github.intel.com/hpdd/lustre/llapi/layout"
	"github.intel.com/hpdd/lustre/pkg/xattr"
)

var (
	fileinfo bool
	filename bool
)

func init() {
	flag.BoolVar(&fileinfo, "i", false, " print file info")
	flag.BoolVar(&filename, "f", false, "always print file name")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s <path>...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	for _, name := range flag.Args() {
		// Use llapi.layout to fetch lov metadata (uses lustre.lov EA)
		l, err := layout.GetByPath(name)
		if err != nil {
			log.Fatal(err)
		}
		// index, _ := l.OstIndex(0)
		fmt.Println("Using llapi_layout_get_by_path")
		fmt.Printf("lmm_stripe_count:   %d\n", l.StripeCount())
		fmt.Printf("lmm_stripe_size:    %d\n", l.StripeSize())
		fmt.Printf("lmm_pattern:        0x%x\n", l.Pattern())
		l.Free()

		// Fetch directly from EA
		b1 := make([]byte, 256)
		sz, err := xattr.Lgetxattr(name, "lustre.lov", b1)
		if err != nil {
			log.Fatal(err)
		}
		lovBuf := b1[0:sz]
		fmt.Println("\nDirectly from lustre.lov EA")
		lum := (*C.struct_lov_user_md)(unsafe.Pointer(&lovBuf[0]))
		fmt.Printf("lmm_magic:          0x%x\n", lum.lmm_magic)
		fmt.Printf("lmm_stripe_count:   %d\n", lum.lmm_stripe_count)
		fmt.Printf("lmm_stripe_size:    %d\n", lum.lmm_stripe_size)
		fmt.Printf("lmm_pattern:        0x%x\n", lum.lmm_pattern)

		// using IOC_MDC_GETSTRIPE (like lfs does)
		cPath := C.CString(name)
		maxLumSize := C.lov_user_md_size(C.LOV_MAX_STRIPE_COUNT, C.LOV_USER_MAGIC_V3)
		buf := make([]byte, maxLumSize)
		lum = (*C.struct_lov_user_md)(unsafe.Pointer(&buf[0]))

		rc, err := C.llapi_file_get_stripe(cPath, lum)
		C.free(unsafe.Pointer(cPath))
		if err != nil {
			log.Fatal(err)
		}
		if rc < 0 {
			log.Fatal("null lum")
		}
		fmt.Println("\nUsing IOC_MDC_GETSTRIPE via llapi_file_get_stripe")
		fmt.Printf("lmm_magic:          0x%x\n", lum.lmm_magic)
		fmt.Printf("lmm_stripe_count:   %d\n", lum.lmm_stripe_count)
		fmt.Printf("lmm_stripe_size:    %d\n", lum.lmm_stripe_size)
		fmt.Printf("lmm_pattern:        0x%x\n", lum.lmm_pattern)
	}

}

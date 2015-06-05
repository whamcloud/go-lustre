package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.intel.com/hpdd/lustre/llapi/layout"
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
		l, err := layout.GetByPath(name)
		if err != nil {
			log.Fatal(err)
		}
		index, _ := l.OstIndex(0)
		fmt.Printf("count:%v size:%v pattern:%v index:%v %s\n", l.StripeCount(), l.StripeSize(), l.Pattern(), index, name)
		l.Free()
	}

}

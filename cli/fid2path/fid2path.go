// fid2path displays the paths for one or more fids.
//  -mnt <mountpoint> Lustre mount point
//  -link <link nbr>: only print the file at the offset
package main

import (
	"flag"
	"fmt"
	"os"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/fs"
)

var (
	link    int
	mnt     string
	verbose bool
)

func init() {
	flag.IntVar(&link, "link", -1, "Specific link to display")
	flag.StringVar(&mnt, "mnt", "", "Lustre mount point.")
	flag.BoolVar(&verbose, "f", false, "always print filenames")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [--link] [--mnt] <fid>...\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if mnt == "" {
		fmt.Fprintln(os.Stderr, "! The -mnt <mntpoint> option was not specified.")
		flag.Usage()
		os.Exit(1)
	}

	root, err := fs.MountRoot(mnt)
	if err != nil {
		fmt.Fprintln(os.Stderr, "%v", err)
		os.Exit(1)
	}

	for _, fidStr := range flag.Args() {
		fid, err := lustre.ParseFid(fidStr)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if link >= 0 {
			// Make sure to only fetch a single path if user requests it
			p, err := fs.FidPathname(root, fid, link)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if flag.NArg() > 1 || verbose {
				fmt.Printf("%s: ", fid)
			}
			fmt.Println(p)
		} else {
			paths, err := fs.FidPathnames(root, fid)
			if err != nil {
				fmt.Println(err)
				continue
			}
			for _, p := range paths {
				if flag.NArg() > 1 || verbose {
					fmt.Printf("%s: ", fid)
				}
				fmt.Println(p)
			}
		}
	}
}

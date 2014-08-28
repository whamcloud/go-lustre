package lustre

import (
	"flag"
	"testing"
)

var mntPath string

func init() {
	flag.StringVar(&mntPath, "mnt", "", "Lustre mountpoint")
}

func TestVersion(t *testing.T) {
	if mntPath == "" {
		t.Fatal("use --mnt <path> to run lustre tests")
	}
	version := Version()
	if version == "" {
		t.Error("Unable to get lustre version.")
	}
}

func TestMountId(t *testing.T) {
	if mntPath == "" {
		t.Fatal("use --mnt <path> to run lustre tests")
	}
	_, err := MountId(mntPath)
	if err != nil {
		t.Error(err)
	}
}

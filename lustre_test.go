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

func TestMountRoot(t *testing.T) {
	_, err := MountRoot(mntPath)
	if err != nil {
		t.Fatal(err)
	}

	_, err = MountRoot("/usr/share/man")
	if err == nil {
		t.Fatal("oops /usr/share/man is not Lustre")
	}
	_, err = MountRoot("/proc/fs/lustre")
	if err == nil {
		t.Fatal("oops /proc/fs/lustre is not Lustre")
	}
	_, err = MountRoot("/dev/pts")
	if err == nil {
		t.Fatal("oops /dev/pts is not Lustre")
	}
}

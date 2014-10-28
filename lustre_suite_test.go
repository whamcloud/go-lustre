package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"

	"errors"
	"fmt"
	// "net"
	"bytes"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"testing"
	"time"
)

type (
	LustreTarget struct {
		name           string
		path           string
		size           int64
		umountPriority int
	}

	MountPoint struct {
		path     string
		priority int
	}

	MountPoints []*MountPoint
)

const (
	CLIENT_PRI = iota
	MDT_PRI    = iota + 1
	OST_PRI    = iota + 2
	MGS_PRI    = iota + 3
)

const (
	TestPrefix = "/tmp/goluTest"
	TestFsName = "goluTest"
)

var CopytoolCLI string
var ClientMount = fmt.Sprintf("%s/client", TestPrefix)

func (m MountPoints) Len() int           { return len(m) }
func (m MountPoints) Swap(i, j int)      { m[i], m[j] = m[j], m[i] }
func (m MountPoints) Less(i, j int) bool { return m[i].priority < m[j].priority }

func shell(name string, arg ...string) (string, error) {
	cmd := CL(name, arg...).Command()
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return "", err
	}
	return out.String(), nil
}

type CmdLine struct {
	Name string
	Args []string
}

func CL(name string, arg ...string) *CmdLine {
	return &CmdLine{Name: name, Args: arg}
}

func (c *CmdLine) Add(arg ...string) {
	c.Args = append(c.Args, arg...)
}

func (c *CmdLine) Command() *exec.Cmd {
	fmt.Fprintf(GinkgoWriter, "+ %s %s\n", c.Name, strings.Join(c.Args, " "))
	return exec.Command(c.Name, c.Args...)
}

func CreateLoopFile(loopPath string, loopSize int64) error {
	err := os.MkdirAll(path.Dir(loopPath), 0755)
	if err != nil {
		return err
	}

	outFile, err := os.Create(loopPath)
	if err != nil {
		return err
	}
	defer outFile.Close()
	outFile.Seek(loopSize-1, 0)
	outFile.Write([]byte{0})
	return nil
}

func loadModules() {
	cmd := CL("modprobe", "lustre").Command()
	session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
	Ω(err).ShouldNot(HaveOccurred())
	session.Wait(60 * time.Second)
	fmt.Fprintf(GinkgoWriter, "Done.\n")
}

func unloadModules() {
	cmd := CL("lustre_rmmod").Command()
	session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
	Ω(err).ShouldNot(HaveOccurred())
	session.Wait(60 * time.Second)
	fmt.Fprintf(GinkgoWriter, "Done.\n")
}

func GetMgsNid() (string, error) {
	s, err := shell("lctl", "list_nids")
	if err != nil {
		return "", err
	}
	return strings.Trim(s, "\n"), nil

	// leave this just in case above doesn't work
	// addrs, err := net.InterfaceAddrs()
	// if err != nil {
	// return "", err
	// }
	// if ipnet, ok := addrs[1].(*net.IPNet); ok {
	// return fmt.Sprintf("%s@tcp", ipnet.IP.String()), nil
	// }
	// return "", errors.New("Unknown error resolving MGS NID.")
}

func DoClientMounts(clientMounts []string, mounts *[]*MountPoint) error {
	mgsNid, err := GetMgsNid()
	if err != nil {
		panic(err)
	}
	mountSpec := fmt.Sprintf("%s:/%s", mgsNid, TestFsName)

	for _, mountPoint := range clientMounts {
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			return err
		}
		cmd := CL("mount", "-tlustre", "-ouser_xattr", mountSpec, mountPoint).Command()
		fmt.Fprintf(GinkgoWriter, "Mounting %s at %s... ", mountSpec, mountPoint)
		session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		session.Wait(60 * time.Second)
		fmt.Fprintf(GinkgoWriter, "Done.\n")
		if mounts != nil {
			*mounts = append(*mounts, &MountPoint{mountPoint, CLIENT_PRI})
		}
	}

	return nil
}

func DoTargetMounts(targets []LustreTarget, mounts *[]*MountPoint) error {
	for _, t := range targets {
		mountPoint := path.Join(TestPrefix, t.name+"Mount")
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			panic(err)
		}
		cmd := CL("mount", "-oloop", "-tlustre", t.path, mountPoint).Command()
		fmt.Fprintf(GinkgoWriter, "Mounting %s at %s... ", t.path, mountPoint)
		session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		session.Wait(60 * time.Second)
		fmt.Fprintf(GinkgoWriter, "Done.\n")
		*mounts = append(*mounts, &MountPoint{mountPoint, t.umountPriority})
	}

	return nil
}

func Unmount(mountPoint string) error {
	cmd := CL("umount", mountPoint).Command()
	fmt.Fprintf(GinkgoWriter, "Unmounting %s... ", mountPoint)
	session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
	Ω(err).ShouldNot(HaveOccurred())
	session.Wait(60 * time.Second)
	fmt.Fprintf(GinkgoWriter, "Done.\n")
	return nil
}

func DoUnmounts(paths []string) error {
	for _, mountPoint := range paths {
		Unmount(mountPoint)
	}

	return nil
}

func DoLustreSetup(targets []LustreTarget) (mounts []*MountPoint) {
	mgsNid, err := GetMgsNid()
	if err != nil {
		panic(err)
	}
	targetCount := make(map[string]int)

	for _, t := range targets {
		mkfs := CL("mkfs.lustre")
		info, err := os.Stat(t.path)
		if err == nil {
			fmt.Fprintf(GinkgoWriter, "Loop file %s already exists: %v\n", t.path, info)
		} else {
			fmt.Fprintf(GinkgoWriter, "Creating %s for %s... ", t.path, t.name)
			if err := CreateLoopFile(t.path, t.size); err != nil {
				panic(err)
			}
			fmt.Fprintf(GinkgoWriter, "Done.\n")
		}

		switch {
		case strings.Index(t.name, "mgs") >= 0:
			if targetCount["mgs"] > 0 {
				panic(errors.New("Too many MGSes!"))
			}
			targetCount["mgs"]++
			mkfs.Add("--mgs")
		case strings.Index(t.name, "mdt") >= 0:
			mkfs.Add("--mdt")
			mkfs.Add(fmt.Sprintf("--index=%d", targetCount["mdt"]))
			mkfs.Add("--fsname", TestFsName)
			mkfs.Add("--mgsnode", mgsNid)

			targetCount["mdt"]++
		case strings.Index(t.name, "ost") >= 0:
			mkfs.Add("--ost")
			mkfs.Add(fmt.Sprintf("--index=%d", targetCount["ost"]))
			mkfs.Add("--fsname", TestFsName)
			mkfs.Add("--mgsnode", mgsNid)

			targetCount["ost"]++
		default:
			panic(errors.New("Unknown target type"))
		}
		mkfs.Add(fmt.Sprintf("--device-size=%d", t.size/1024))
		mkfs.Add(t.path)

		cmd := mkfs.Command()
		session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		session.Wait(60 * time.Second)
	}

	if err := DoTargetMounts(targets, &mounts); err != nil {
		panic(err)
	}

	if err := DoClientMounts([]string{ClientMount}, &mounts); err != nil {
		panic(err)
	}
	return mounts
}

func DoLustreTeardown(mounts []*MountPoint) {
	sort.Sort(MountPoints(mounts))
	var unmountList []string
	for _, m := range mounts {
		unmountList = append(unmountList, m.path)
	}
	DoUnmounts(unmountList)

	if err := os.RemoveAll(TestPrefix); err != nil {
		panic(err)
	}
}

func ToggleHsmCoordinatorState(state string) error {
	cdtControlFile := fmt.Sprintf("/proc/fs/lustre/mdt/%s-MDT0000/hsm_control", TestFsName)
	f, err := os.OpenFile(cdtControlFile, os.O_WRONLY, 0400)
	defer f.Close()
	if err != nil {
		return err
	}
	if _, err := f.WriteString(state); err != nil {
		return err
	}

	return nil
}

func TestLustre(t *testing.T) {
	targets := []LustreTarget{
		{"mgs", path.Join(TestPrefix, "mgsLoopFile"), 128 * 1e6, MGS_PRI},
		{"mdt00", path.Join(TestPrefix, "mdt00LoopFile"), 512 * 1e6, MDT_PRI},
		{"ost00", path.Join(TestPrefix, "ost00LoopFile"), 1024 * 1e6, OST_PRI},
	}
	var activeMounts []*MountPoint

	BeforeSuite(func() {
		var err error
		CopytoolCLI, err = Build("hpdd/cmds/copytool")
		Ω(err).ShouldNot(HaveOccurred())
		loadModules()
		activeMounts = DoLustreSetup(targets)
		ToggleHsmCoordinatorState("enabled")
	})

	AfterSuite(func() {
		CleanupBuildArtifacts()
		DoLustreTeardown(activeMounts)
		unloadModules()
	})

	RegisterFailHandler(Fail)
	RunSpecs(t, "Lustre Suite")
}

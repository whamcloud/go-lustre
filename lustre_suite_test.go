package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"

	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"
)

type (
	LustreTarget struct {
		path string
		size int64
	}
)

var TestPrefix = "/tmp/goluTest"
var TestFsName = "goluTest"
var CopytoolCLI string

// Could just exec dd, but where would the fun be in that?
// I'm tryin' to learn something here!
func CreateLoopFile(loopPath string, loopSize int64) error {
	var err error
	var inFile, outFile *os.File
	var totalRead int64
	readBuffer := make([]byte, 1e6)

	if err := os.MkdirAll(path.Dir(loopPath), 0755); err != nil {
		return err
	}

	if inFile, err = os.Open("/dev/zero"); err != nil {
		return err
	}
	defer inFile.Close()

	if outFile, err = os.Create(loopPath); err != nil {
		return err
	}
	defer outFile.Close()

	for {
		n, err := inFile.Read(readBuffer)
		if err != nil {
			return err
		}
		if totalRead+int64(n) > loopSize {
			n = int(loopSize - (totalRead + int64(n)))
		}
		if n <= 0 {
			break
		}
		totalRead += int64(n)
		if _, err := outFile.Write(readBuffer[:n]); err != nil {
			return err
		}
	}

	return nil
}

func GetMgsNid() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	if ipnet, ok := addrs[1].(*net.IPNet); ok {
		return fmt.Sprintf("%s@tcp", ipnet.IP.String()), nil
	}
	return "", errors.New("Unknown error resolving MGS NID.")
}

func DoClientMounts(clientMounts *map[string]string) error {
	clientMountCommand := []string{"mount", "-tlustre", "", ""}
	for mountSpec, mountPoint := range *clientMounts {
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			return err
		}
		clientMountCommand[2] = mountSpec
		clientMountCommand[3] = mountPoint
		fmt.Fprintf(GinkgoWriter, "Mounting %s at %s... ", mountSpec, mountPoint)
		cmd := exec.Command(clientMountCommand[0], clientMountCommand[1:]...)
		session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		session.Wait(60 * time.Second)
		fmt.Fprintf(GinkgoWriter, "Done.\n")
	}

	return nil
}

func DoTargetMounts(mountPoints *map[string]string, blockDevices *map[string]*LustreTarget) error {
	mountCommand := []string{"mount", "-oloop", "-tlustre", "", ""}
	for target, mountPoint := range *mountPoints {
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			panic(err)
		}
		mountCommand[3] = (*blockDevices)[target].path
		mountCommand[4] = mountPoint
		fmt.Fprintf(GinkgoWriter, "Mounting %s at %s... ", (*blockDevices)[target].path, mountPoint)
		cmd := exec.Command(mountCommand[0], mountCommand[1:]...)
		session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		session.Wait(60 * time.Second)
		fmt.Fprintf(GinkgoWriter, "Done.\n")
	}

	return nil
}

func DoUnmounts(mountPoints *[]string) error {
	umountCommand := []string{"umount", ""}
	for _, mountPoint := range *mountPoints {
		umountCommand[1] = mountPoint
		fmt.Fprintf(GinkgoWriter, "Unmounting %s... ", mountPoint)
		cmd := exec.Command(umountCommand[0], umountCommand[1:]...)
		session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		session.Wait(60 * time.Second)
		fmt.Fprintf(GinkgoWriter, "Done.\n")
	}

	return nil
}

func DoLustreSetup(blockDevices *map[string]*LustreTarget,
	mountPoints *map[string]string) {

	mgsNid, err := GetMgsNid()
	if err != nil {
		panic(err)
	}
	targetCount := make(map[string]int)
	mkfsCommand := []string{"mkfs.lustre", "", "", "", "", "", ""}

	for target, loopFile := range *blockDevices {
		mkfsArgs := mkfsCommand[1:]
		info, err := os.Stat(loopFile.path)
		if err == nil {
			fmt.Fprintf(GinkgoWriter, "Loop file %s already exists: %v\n", loopFile.path, info)
		} else {
			fmt.Fprintf(GinkgoWriter, "Creating %s for %s... ", loopFile.path, target)
			if err := CreateLoopFile(loopFile.path, loopFile.size); err != nil {
				panic(err)
			}
			fmt.Fprintf(GinkgoWriter, "Done.\n")
		}

		switch {
		case strings.Index(target, "mgs") >= 0:
			if targetCount["mgs"] > 0 {
				panic(errors.New("Too many MGSes!"))
			}
			targetCount["mgs"]++
			mkfsCommand[1] = "--mgs"
			mkfsCommand[2] = fmt.Sprintf("--device-size=%d", (*blockDevices)[target].size/1024)
			mkfsCommand[3] = loopFile.path
			mkfsArgs = mkfsCommand[1:4]
		case strings.Index(target, "mdt") >= 0:
			mkfsCommand[1] = "--mdt"
			mkfsCommand[2] = fmt.Sprintf("--index=%d", targetCount["mdt"])
			mkfsCommand[3] = fmt.Sprintf("--fsname=%s", TestFsName)
			mkfsCommand[4] = fmt.Sprintf("--mgsnode=%s", mgsNid)

			targetCount["mdt"]++
		case strings.Index(target, "ost") >= 0:
			mkfsCommand[1] = "--ost"
			mkfsCommand[2] = fmt.Sprintf("--index=%d", targetCount["ost"])
			mkfsCommand[3] = fmt.Sprintf("--fsname=%s", TestFsName)
			mkfsCommand[4] = fmt.Sprintf("--mgsnode=%s", mgsNid)

			targetCount["ost"]++
		default:
			panic(errors.New("Unknown target type"))
		}
		mkfsCommand[5] = fmt.Sprintf("--device-size=%d", (*blockDevices)[target].size/1024)
		mkfsCommand[6] = loopFile.path

		cmd := exec.Command(mkfsCommand[0], mkfsArgs...)
		session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
		Ω(err).ShouldNot(HaveOccurred())
		session.Wait(60 * time.Second)
	}

	if err := DoTargetMounts(mountPoints, blockDevices); err != nil {
		panic(err)
	}

	clientMountSpec := fmt.Sprintf("%s:/%s", mgsNid, TestFsName)
	clientMountPoint := fmt.Sprintf("%s/client", TestPrefix)
	clientMounts := map[string]string{clientMountSpec: clientMountPoint}
	if err := DoClientMounts(&clientMounts); err != nil {
		panic(err)
	}
}

func DoLustreTeardown(blockDevices *map[string]*LustreTarget, mountPoints *map[string]string) {

	(*mountPoints)["client"] = fmt.Sprintf("%s/client", TestPrefix)
	//mountPoints["copytool"] = fmt.Sprintf("%s/client/.ct", TestPrefix)
	unmountList := make([]string, 0, len(*mountPoints))
	for _, mountPoint := range *mountPoints {
		unmountList = append(unmountList, mountPoint)
	}
	DoUnmounts(&unmountList)

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
	blockDevices := map[string]*LustreTarget{
		"mgs":   {path.Join(TestPrefix, "mgsLoopFile"), 128 * 1e6},
		"mdt00": {path.Join(TestPrefix, "mdt00LoopFile"), 512 * 1e6},
		"ost00": {path.Join(TestPrefix, "ost00LoopFile"), 1024 * 1e6},
	}
	mountPoints := map[string]string{
		"mgs":   path.Join(TestPrefix, "mgsMount"),
		"mdt00": path.Join(TestPrefix, "mdt00Mount"),
		"ost00": path.Join(TestPrefix, "ost00Mount"),
	}

	BeforeSuite(func() {
		var err error
		CopytoolCLI, err = Build("hpdd/cmds/copytool")
		Ω(err).ShouldNot(HaveOccurred())

		DoLustreSetup(&blockDevices, &mountPoints)
		ToggleHsmCoordinatorState("enabled")
	})

	AfterSuite(func() {
		CleanupBuildArtifacts()
		DoLustreTeardown(&blockDevices, &mountPoints)
	})

	RegisterFailHandler(Fail)
	RunSpecs(t, "Lustre Suite")
}

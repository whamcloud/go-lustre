package lustre_test

import (
	"fmt"
	"github.com/AlekSi/xattr"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"os"
	"os/exec"
	"path"
	"syscall"
	"time"
)

// TODO: Figure out global shared config.
var CopytoolMount = fmt.Sprintf("%s.ct", ClientMount)
var HsmArchive = fmt.Sprintf("%s/archive", TestPrefix)
var CopytoolCmd *exec.Cmd

func DoCopytoolSetup(backendType string) error {
	if err := os.MkdirAll(HsmArchive, 0755); err != nil {
		return err
	}

	if err := DoClientMounts([]string{CopytoolMount}, nil); err != nil {
		return err
	}

	posixArchive := fmt.Sprintf("%s:%s:1::%s:false", backendType, backendType, HsmArchive)
	CopytoolCmd = CL(CopytoolCLI, "--disable-mirror", "--archive", posixArchive, "--mnt", CopytoolMount).Command()
	if _, err := Start(CopytoolCmd, GinkgoWriter, GinkgoWriter); err != nil {
		return err
	}

	return nil
}

func DoCopytoolTeardown() error {
	if CopytoolCmd.Process != nil {
		if err := CopytoolCmd.Process.Kill(); err != nil {
			return err
		}
		CopytoolCmd.Wait()
	}

	Unmount(CopytoolMount)

	if err := os.RemoveAll(HsmArchive); err != nil {
		return err
	}

	return nil
}

func MarkFileForHsmAction(targetFile string, hsmAction string) error {
	args := make([]string, 0, 4)
	switch hsmAction {
	case "archive":
		args = append(args, "hsm_archive", "--archive", "1", targetFile)
	case "restore":
		args = append(args, "hsm_restore", targetFile)
	case "release":
		args = append(args, "hsm_release", targetFile)
	default:
		panic(fmt.Sprintf("Unknown HSM action: %s", hsmAction))
	}

	cmd := CL("lfs", args...).Command()
	session, err := Start(cmd, GinkgoWriter, GinkgoWriter)
	if err != nil {
		return err
	}
	session.Wait(60 * time.Second)

	return nil
}

var _ = Describe("When HSM is enabled,", func() {
	Describe("the POSIX Copytool", func() {
		var testFiles = make([]string, 3)
		testFiles[0] = path.Join(ClientMount, "foo")
		testFiles[1] = path.Join(ClientMount, "bar")
		testFiles[2] = path.Join(ClientMount, "baz")

		BeforeEach(func() {
			if err := DoCopytoolSetup("posix"); err != nil {
				panic(err)
			}

			for _, file := range testFiles {
				f, err := os.Create(file)
				if err != nil {
					panic(err)
				}
				// Write the filename as "content".
				if _, err = f.WriteString(file); err != nil {
					panic(err)
				}
				f.Close()
			}
		})
		AfterEach(func() {
			if err := DoCopytoolTeardown(); err != nil {
				panic(err)
			}

			for _, file := range testFiles {
				if err := os.Remove(file); err != nil {
					panic(err)
				}
			}
		})

		// FIXME: Not really happy about the fact that these tests
		// rely on knowing way too much about the backend
		// implementation.
		Describe("responds to an archive request", func() {
			It("by copying the file into the archive.", func() {
				Ω(MarkFileForHsmAction(testFiles[0], "archive")).Should(Succeed())
				Eventually(func() bool {
					uuid, err := xattr.Get(testFiles[0], "hsm_id")
					if err != nil {
						return false
					}

					uuidStr := string(uuid)
					archiveFile := path.Join(HsmArchive,
						"objects",
						fmt.Sprintf("%s", uuidStr[0:2]),
						fmt.Sprintf("%s", uuidStr[2:4]),
						uuidStr)

					_, err = os.Stat(archiveFile)
					return err == nil
				}, 60*time.Second).Should(BeTrue())
			})
		})

		Describe("responds to a restore request", func() {
			testFile := testFiles[0]
			var stat syscall.Stat_t
			var f *os.File
			var fd int
			var err error

			BeforeEach(func() {
				f, err = os.Open(testFile)
				Ω(err).ShouldNot(HaveOccurred())
				fd = int(f.Fd())

				Ω(MarkFileForHsmAction(testFile, "archive")).Should(Succeed())
				Ω(MarkFileForHsmAction(testFile, "release")).Should(Succeed())
				// LU-3684: A released file will report 0 or 1
				// blocks, depending on the Lustre version.
				Eventually(func() bool {
					err := syscall.Fstat(fd, &stat)
					Ω(err).ShouldNot(HaveOccurred())
					return stat.Blocks <= 1
				}, 300*time.Second, time.Second).Should(BeTrue())
			})
			AfterEach(func() {
				f.Close()
			})
			It("by restoring the file contents from the archive.", func() {
				Ω(MarkFileForHsmAction(testFile, "restore")).Should(Succeed())
				Eventually(func() bool {
					err := syscall.Fstat(fd, &stat)
					Ω(err).ShouldNot(HaveOccurred())
					return stat.Blocks > 1
				}, 60*time.Second, time.Second).Should(BeTrue())
			})
		})
	})
})

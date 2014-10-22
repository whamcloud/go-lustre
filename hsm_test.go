package lustre_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"
)

// TODO: Figure out global shared config.
var ClientMount = fmt.Sprintf("%s/client", TestPrefix)
var CopytoolMount = fmt.Sprintf("%s/.ct", ClientMount)
var HsmArchive = fmt.Sprintf("%s/archive", TestPrefix)
var CopytoolCmd *exec.Cmd

func DoCopytoolSetup(backendType string) error {
	if err := os.MkdirAll(HsmArchive, 0755); err != nil {
		return err
	}

	mgsNid, err := GetMgsNid()
	if err != nil {
		return err
	}
	ctMountSpec := fmt.Sprintf("%s:/%s", mgsNid, TestFsName)
	copytoolMounts := map[string]string{ctMountSpec: CopytoolMount}
	if err := DoClientMounts(&copytoolMounts); err != nil {
		return err
	}

	posixArchive := fmt.Sprintf("%s:%s:1::%s:false", backendType, backendType, HsmArchive)
	copytoolCommand := []string{CopytoolCLI, "--archive", posixArchive, "--mnt", CopytoolMount}
	fmt.Fprintf(GinkgoWriter, "Running %s", strings.Join(copytoolCommand, " "))
	CopytoolCmd := exec.Command(copytoolCommand[0], copytoolCommand...)
	if _, err := Start(CopytoolCmd, GinkgoWriter, GinkgoWriter); err != nil {
		return err
	}
	//time.Sleep(60 * time.Second)

	return nil
}

func DoCopytoolTeardown() error {
	if CopytoolCmd.Process != nil {
		if err := CopytoolCmd.Process.Kill(); err != nil {
			return err
		}
		CopytoolCmd.Wait()
	}

	unmountList := []string{CopytoolMount}
	if err := DoUnmounts(&unmountList); err != nil {
		return err
	}

	if err := os.RemoveAll(HsmArchive); err != nil {
		return err
	}

	return nil
}

func MarkFileForArchive(targetFile string) error {
	archiveCommand := []string{"lfs", "hsm_archive", "--archive", "1", targetFile}
	cmd := exec.Command(archiveCommand[0], archiveCommand[1:]...)
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

		Describe("responds to an archive request", func() {
			It("by copying the file into the archive.", func() {
				Ω(MarkFileForArchive(testFiles[0])).Should(Succeed())
				Eventually(func() bool {
					_, err := os.Stat(testFiles[0])
					return err == nil
				}, 60*time.Second).Should(BeTrue())
			})
		})
	})
})

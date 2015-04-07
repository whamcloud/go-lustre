package changelog_test

import (
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/changelog"
	"github.intel.com/hpdd/test/harness"
	"github.intel.com/hpdd/test/log"
	"github.intel.com/hpdd/test/utils"
)

var _ = Describe("When Changelogs are enabled", func() {
	var changelogUser string
	var changelogMdt string

	BeforeEach(func() {
		var err error
		changelogUser, changelogMdt, err = harness.RegisterChangelogUser()
		Ω(err).ShouldNot(HaveOccurred())
	})
	AfterEach(func() {
		err := changelog.Clear(changelogMdt, changelogUser, 0)
		Ω(err).ShouldNot(HaveOccurred())

		err = harness.DeregisterChangelogUser(changelogUser, changelogMdt)
		Ω(err).ShouldNot(HaveOccurred())
	})
	Describe("creating a file", func() {
		fileName := "new-file"
		var testFile string
		BeforeEach(func() {
			testFile = utils.CreateTestFile(fileName)
		})
		AfterEach(func() {
			err := os.Remove(testFile)
			Ω(err).ShouldNot(HaveOccurred())
		})
		It("should result in a CREAT changelog record.", func() {
			var rec lustre.ChangelogRecord
			var err error
			Eventually(func() lustre.ChangelogRecord {
				h := changelog.CreateHandle(changelogMdt)
				defer h.Close()

				err = h.Open(false)
				Ω(err).ShouldNot(HaveOccurred())

				rec, err = h.NextRecord()
				return rec
			}, 5*time.Second).ShouldNot(BeNil())
			Ω(err).ShouldNot(HaveOccurred())
			log.Debug(rec.String())
			Expect(rec.Type()).To(Equal("CREAT"))
			Expect(rec.Name()).To(Equal(fileName))
		})
	})
	Describe("renaming a file", func() {
		fileName := "old-file"
		newFileName := "renamed-file"
		var testFile string
		BeforeEach(func() {
			testFile = utils.CreateTestFile(fileName)
		})
		AfterEach(func() {
			err := os.Remove(testFile)
			Ω(err).ShouldNot(HaveOccurred())
		})
		It("should result in a RENME changelog entry.", func() {
			oldFile := testFile
			testFile = strings.Replace(testFile, fileName, newFileName, 1)
			err := os.Rename(oldFile, testFile)
			Ω(err).ShouldNot(HaveOccurred())
			log.Debug("Renamed %s -> %s", oldFile, testFile)

			var rec lustre.ChangelogRecord
			f := changelog.CreateFollower(changelogMdt, 0)
			defer f.Close()
			getRename := func() lustre.ChangelogRecord {
				rec, err = f.NextRecord()
				for ; err == nil; rec, err = f.NextRecord() {
					if rec.Type() == "RENME" {
						return rec
					}
				}
				return nil
			}

			Eventually(getRename, 5*time.Second, time.Second).ShouldNot(BeNil())

			log.Debug(rec.String())
			Expect(rec.Name()).To(Equal(newFileName))
		})
	})
	Describe("shutting down Follower", func() {
		testFiles := []string{"a", "b", "c", "d", "e", "f"}
		BeforeEach(func() {
			for _, testFile := range testFiles {
				utils.CreateTestFile(testFile)
			}
		})
		AfterEach(func() {
			for _, testFile := range testFiles {
				err := os.Remove(utils.TestFilePath(testFile))
				Ω(err).ShouldNot(HaveOccurred())
			}
		})
		It("should stop processing records immediately.", func() {
			f := changelog.CreateFollower(changelogMdt, 0)

			for i := range testFiles {
				if i == 4 {
					f.Close()
				}

				rec, err := f.NextRecord()
				if i < 4 {
					Ω(err).ShouldNot(HaveOccurred())
					log.Debug(rec.String())
					Expect(rec.Name()).To(Equal(testFiles[i]))
				} else {
					Ω(err).Should(HaveOccurred())
				}
			}
		})
	})
	Describe("Follower should keep reading when new CLs are available", func() {
		testFiles := []string{"a", "b", "c", "d", "e", "f"}
		BeforeEach(func() {
			for _, testFile := range testFiles[:len(testFiles)-1] {
				utils.CreateTestFile(testFile)
			}
		})
		AfterEach(func() {
			for _, testFile := range testFiles {
				err := os.Remove(utils.TestFilePath(testFile))
				Ω(err).ShouldNot(HaveOccurred())
			}
		})
		It("and should block until a new record is available.", func() {
			f := changelog.CreateFollower(changelogMdt, 0)
			defer f.Close()

			for i := range testFiles[:len(testFiles)-1] {
				rec, err := f.NextRecord()
				Ω(err).ShouldNot(HaveOccurred())
				log.Debug(rec.String())
				Expect(rec.Name()).To(Equal(testFiles[i]))
			}

			lastIdx := len(testFiles) - 1
			go func() {
				time.Sleep(1 * time.Second)
				utils.CreateTestFile(testFiles[lastIdx])
			}()

			rec, err := f.NextRecord()
			Ω(err).ShouldNot(HaveOccurred())
			Expect(rec.Name()).To(Equal(testFiles[lastIdx]))
		})
	})

})

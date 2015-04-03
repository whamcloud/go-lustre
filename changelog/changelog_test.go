package changelog_test

import (
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/lustre/changelog"
	"github.intel.com/hpdd/test/harness"
	"github.intel.com/hpdd/test/log"
	"github.intel.com/hpdd/test/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"os"
	"strings"
	"time"
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
			var nextIndex int64
			h := changelog.CreateHandle(changelogMdt)
			getRename := func() lustre.ChangelogRecord {
				err = h.OpenAt(nextIndex, false)
				Ω(err).ShouldNot(HaveOccurred())
				defer h.Close()

				rec, err = h.NextRecord()
				for err == nil {
					if rec.Type() == "RENME" {
						return rec
					}
					rec, err = h.NextRecord()
				}
				return nil
			}

			Eventually(getRename, 5*time.Second, time.Second).ShouldNot(BeNil())
			//f.Close()

			log.Debug(rec.String())
			Expect(rec.Name()).To(Equal(newFileName))
		})
	})
})

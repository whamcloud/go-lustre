package lustre_test

import (
	"github.intel.com/hpdd/lustre"
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
		err := lustre.ChangelogClear(changelogMdt, changelogUser, 0)
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
		It("should result in a CREAT changelog entry.", func() {
			var entry *lustre.ChangelogEntry = nil
			Eventually(func() *lustre.ChangelogEntry {
				changelog := lustre.ChangelogOpen(harness.ClientMount(), false, 0)
				Ω(changelog).ShouldNot(BeNil())
				defer changelog.Close()
				entry = changelog.GetNextLogEntry()
				return entry
			}, 5*time.Second).ShouldNot(BeNil())
			log.Debug(entry.String())
			Expect(entry.TypeName).To(Equal("CREAT"))
			Expect(entry.Name).To(Equal(fileName))
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

			var entry *lustre.ChangelogEntry
			var nextIndex int64
			getRename := func() *lustre.ChangelogEntry {
				changelog := lustre.ChangelogOpen(harness.ClientMount(), false, nextIndex)
				Ω(changelog).ShouldNot(BeNil())
				for entry = changelog.GetNextLogEntry(); entry != nil; entry = changelog.GetNextLogEntry() {
					if entry.TypeName == "RENME" {
						return entry
					}
					nextIndex = entry.Index + 1
				}
				changelog.Close()
				return nil
			}

			Eventually(getRename, 5*time.Second, time.Second).ShouldNot(BeNil())
			log.Debug(entry.String())
			Expect(entry.Name).To(Equal(newFileName))
		})
	})
})

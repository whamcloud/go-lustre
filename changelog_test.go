package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "hpdd/lustre"
	"hpdd/test/harness"

	"os"
	"path"
)

var _ = Describe("When Changelogs are enabled", func() {
	var changelogUser string
	var changelogMdt string
	var changelog *Changelog

	BeforeEach(func() {
		var err error
		changelogUser, changelogMdt, err = harness.RegisterChangelogUser()
		Ω(err).ShouldNot(HaveOccurred())
		changelog = ChangelogOpen(harness.ClientMount(), false, 0)
		Ω(changelog).ShouldNot(BeNil())
	})
	AfterEach(func() {
		err := harness.ClearChangelogs(changelogUser, changelogMdt)
		Ω(err).ShouldNot(HaveOccurred())

		err = harness.DeregisterChangelogUser(changelogUser, changelogMdt)
		Ω(err).ShouldNot(HaveOccurred())
		changelog.Close()
	})
	Describe("creating a file", func() {
		fileName := "new-file"
		BeforeEach(func() {
			testFile := path.Join(harness.ClientMount(), fileName)
			f, err := os.Create(testFile)
			defer f.Close()
			Ω(err).ShouldNot(HaveOccurred())
			f.WriteString(testFile)
		})
		AfterEach(func() {
			err := os.Remove(path.Join(harness.ClientMount(), fileName))
			Ω(err).ShouldNot(HaveOccurred())
		})
		It("should result in a CREAT changelog entry.", func() {
			entry := changelog.GetNextLogEntry()
			Ω(entry).ShouldNot(BeNil())
			Expect(entry.TypeName).To(Equal("CREAT"))
			Expect(entry.Name).To(Equal(fileName))
		})
	})
})

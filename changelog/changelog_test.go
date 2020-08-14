// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package changelog_test

import (
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/wastore/go-lustre/changelog"
	"github.com/intel-hpdd/test/harness"
	"github.com/intel-hpdd/test/log"
	"github.com/intel-hpdd/test/utils"
)

func nextCreateRecord(f *changelog.Follower) (changelog.Record, error) {
	rec, err := f.NextRecord()
	for ; err == nil && rec.Type() != "CREAT"; rec, err = f.NextRecord() {
	}
	return rec, err
}

var _ = Describe("When Changelogs are enabled", func() {
	var changelogUser string
	var changelogMdt string

	BeforeEach(func() {
		err := harness.Lock(utils.CurrentTestID(), nil)
		Ω(err).ShouldNot(HaveOccurred())

		changelogUser, changelogMdt, err = harness.RegisterChangelogUser()
		Ω(err).ShouldNot(HaveOccurred())
	})
	AfterEach(func() {
		Ω(changelog.Clear(changelogMdt, changelogUser, 0)).Should(Succeed())
		Ω(harness.DeregisterChangelogUser(changelogUser, changelogMdt)).Should(Succeed())
		Ω(harness.Unlock(utils.CurrentTestID())).Should(Succeed())
	})
	Describe("creating a file", func() {
		fileName := "new-file"
		var testFile string
		BeforeEach(func() {
			testFile = utils.CreateTestFile(fileName)
		})
		AfterEach(func() {
			Ω(os.Remove(testFile)).Should(Succeed())
		})
		It("should result in a CREAT changelog record.", func() {
			var rec changelog.Record
			var err error
			Eventually(func() changelog.Record {
				h := changelog.CreateHandle(changelogMdt)
				defer h.Close()

				Ω(h.Open(false)).Should(Succeed())

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
			Ω(os.Remove(testFile)).Should(Succeed())
		})
		It("should result in a RENME changelog entry.", func() {
			oldFile := testFile
			testFile = strings.Replace(testFile, fileName, newFileName, 1)
			err := os.Rename(oldFile, testFile)
			Ω(err).ShouldNot(HaveOccurred())
			log.Debug("Renamed %s -> %s", oldFile, testFile)

			var rec changelog.Record
			f := changelog.CreateFollower(changelogMdt, 0)
			defer f.Close()
			getRename := func() changelog.Record {
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
				Ω(os.Remove(utils.TestFilePath(testFile))).Should(Succeed())
			}
		})
		It("should stop processing records immediately.", func() {
			f := changelog.CreateFollower(changelogMdt, 0)

			for i := range testFiles {
				if i == 4 {
					f.Close()
				}

				rec, err := nextCreateRecord(f)
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
				Ω(os.Remove(utils.TestFilePath(testFile))).Should(Succeed())
			}
		})
		It("and should block until a new record is available.", func() {
			f := changelog.CreateFollower(changelogMdt, 0)
			defer f.Close()

			for i := range testFiles[:len(testFiles)-1] {
				rec, err := nextCreateRecord(f)
				Ω(err).ShouldNot(HaveOccurred())
				log.Debug(rec.String())
				Expect(rec.Name()).To(Equal(testFiles[i]))
			}

			lastIdx := len(testFiles) - 1
			go func() {
				time.Sleep(1 * time.Second)
				utils.CreateTestFile(testFiles[lastIdx])
			}()

			rec, err := nextCreateRecord(f)
			Ω(err).ShouldNot(HaveOccurred())
			Expect(rec.Name()).To(Equal(testFiles[lastIdx]))
		})
	})

})

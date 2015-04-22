package fs_test

import (
	"os"
	"path"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/test/harness"
	"github.intel.com/hpdd/test/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("In the Lustre API functions,", func() {
	testFiles := []string{"test1", "test2", "test3"}
	BeforeEach(func() {
		for _, file := range testFiles {
			utils.CreateTestFile(file)
		}
	})
	AfterEach(func() {
		for _, file := range testFiles {
			Expect(os.Remove(utils.TestFilePath(file))).To(Succeed())
		}
	})

	Describe("Version()", func() {
		It("should return the current Lustre version.", func() {
			Expect(fs.Version()).ToNot(Equal(""))
		})
	})

	Describe("MountId()", func() {
		It("should not return an error, given a valid mount.", func() {
			id, err := fs.MountID(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			Expect(id.FsName).ToNot(Equal(""))
			Expect(id.ClientID).ToNot(Equal(""))
		})
	})

	Describe("MountRoot()", func() {
		It("should not fail, given a valid path.", func() {
			Expect(fs.MountRoot(harness.ClientMount())).To(BeEquivalentTo(harness.ClientMount()))
		})

		It("should not fail, given a valid file in lustre", func() {
			Expect(fs.MountRoot(utils.TestFilePath(testFiles[0]))).To(BeEquivalentTo(harness.ClientMount()))
		})

		It("should fail, given a non-Lustre path.", func() {
			_, err := fs.MountRoot("/usr/share/man")
			Ω(err).Should(HaveOccurred())
		})
	})

	Describe("MountRelPath()", func() {
		It("should not fail, given a valid pathname.", func() {
			mnt, relPath, err := fs.MountRelPath(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(harness.ClientMount()))
			Expect(string(relPath)).To(Equal(""))
		})

		It("should fail, given a non-Lustre pathname.", func() {
			_, _, err := fs.MountRelPath("/usr/share/man")
			Ω(err).Should(HaveOccurred())
		})

		It("should return mount and relative given pathname in lustre fs.", func() {
			mnt, relPath, err := fs.MountRelPath(path.Join(harness.ClientMount(), testFiles[0]))
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(harness.ClientMount()))
			Expect(string(relPath)).To(Equal(testFiles[0]))
		})
	})

	Describe("GetID()", func() {
		It("should return the fs ID, given a valid mount.", func() {
			Expect(fs.GetID(harness.ClientMount())).To(Equal(harness.FsID()))
		})
	})

})

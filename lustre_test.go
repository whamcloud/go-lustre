package lustre_test

import (
	"hpdd/lustre"
	"hpdd/test/harness"
	"os"
	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("In the Lustre API functions,", func() {
	testFiles := []string{"test1", "test2", "test3"}
	BeforeEach(func() {
		for _, file := range testFiles {
			f, err := os.Create(path.Join(harness.ClientMount(), file))
			if err != nil {
				panic(err)
			}
			f.Close()
		}
	})
	AfterEach(func() {
		for _, file := range testFiles {
			if err := os.Remove(path.Join(harness.ClientMount(), file)); err != nil {
				panic(err)
			}
		}
	})

	Describe("Version()", func() {
		It("should return the current Lustre version.", func() {
			Expect(lustre.Version()).ToNot(Equal(""))
		})
	})

	Describe("MountId()", func() {
		It("should not return an error, given a valid mount.", func() {
			id, err := lustre.MountID(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			Expect(id.FsName).ToNot(Equal(""))
			Expect(id.ClientID).ToNot(Equal(""))
		})
	})

	Describe("MountRoot()", func() {
		It("should not fail, given a valid path.", func() {
			mnt, err := lustre.MountRoot(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(harness.ClientMount()))
		})

		It("should not fail, given a valid file in lustre", func() {
			mnt, err := lustre.MountRoot(path.Join(harness.ClientMount(), testFiles[0]))
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(harness.ClientMount()))
		})

		It("should fail, given a non-Lustre path.", func() {
			_, err := lustre.MountRoot("/usr/share/man")
			Ω(err).Should(HaveOccurred())
		})
	})

	Describe("MountRelPath()", func() {
		It("should not fail, given a valid pathname.", func() {
			mnt, relPath, err := lustre.MountRelPath(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(harness.ClientMount()))
			Expect(string(relPath)).To(Equal(""))
		})

		It("should fail, given a non-Lustre pathname.", func() {
			_, _, err := lustre.MountRelPath("/usr/share/man")
			Ω(err).Should(HaveOccurred())
		})

		It("should return mount and relative given pathname in lustre fs.", func() {
			mnt, relPath, err := lustre.MountRelPath(path.Join(harness.ClientMount(), testFiles[0]))
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(harness.ClientMount()))
			Expect(string(relPath)).To(Equal(testFiles[0]))
		})
	})

	Describe("GetID()", func() {
		It("should return the fs ID, given a valid mount.", func() {
			id, err := lustre.GetID(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			Expect(id).To(Equal(harness.FsID()))
		})
	})

})

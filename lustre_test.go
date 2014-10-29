package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "hpdd/lustre"
	. "hpdd/testlib"
	"os"
	"path"
)

var _ = Describe("In the Lustre API functions,", func() {
	testFiles := []string{"test1", "test2", "test3"}
	BeforeEach(func() {
		for _, file := range testFiles {
			f, err := os.Create(path.Join(ClientMount, file))
			if err != nil {
				panic(err)
			}
			f.Close()
		}
	})
	AfterEach(func() {
		for _, file := range testFiles {
			if err := os.Remove(path.Join(ClientMount, file)); err != nil {
				panic(err)
			}
		}
	})

	Describe("Version()", func() {
		It("should return the current Lustre version.", func() {
			Expect(Version()).ToNot(Equal(""))
		})
	})

	Describe("MountId()", func() {
		It("should not return an error, given a valid mount.", func() {
			id, err := MountId(ClientMount)
			Ω(err).ShouldNot(HaveOccurred())

			Expect(id).ToNot(Equal(""))
		})
	})

	Describe("MountRoot()", func() {
		It("should not fail, given a valid path.", func() {
			mnt, err := MountRoot(ClientMount)
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(ClientMount))
		})

		It("should not fail, given a valid file in lustre", func() {
			mnt, err := MountRoot(path.Join(ClientMount, testFiles[0]))
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(ClientMount))
		})

		It("should fail, given a non-Lustre path.", func() {
			_, err := MountRoot("/usr/share/man")
			Ω(err).Should(HaveOccurred())
		})
	})

	Describe("MountRelPath()", func() {
		It("should not fail, given a valid pathname.", func() {
			mnt, relPath, err := MountRelPath(ClientMount)
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(ClientMount))
			Expect(string(relPath)).To(Equal(""))
		})

		It("should fail, given a non-Lustre pathname.", func() {
			_, _, err := MountRelPath("/usr/share/man")
			Ω(err).Should(HaveOccurred())
		})

		It("should return mount and relative given pathname in lustre fs.", func() {
			mnt, relPath, err := MountRelPath(path.Join(ClientMount, testFiles[0]))
			Ω(err).ShouldNot(HaveOccurred())

			Expect(string(mnt)).To(Equal(ClientMount))
			Expect(string(relPath)).To(Equal(testFiles[0]))
		})
	})

})

package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "hpdd/lustre"
)

var _ = Describe("In the Lustre API functions,", func() {
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

		It("should fail, given a non-Lustre path.", func() {
			_, err := MountRoot("/usr/share/man")
			Ω(err).Should(HaveOccurred())
		})
	})
})

package lustre_test

import (
	"github.intel.com/hpdd/lustre"
	"github.intel.com/hpdd/test/harness"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"
	"io/ioutil"
	"os"
	"path"
)

var _ = Describe("In the FID Utility Library", func() {
	Describe("lookup functions,", func() {
		var testFile *os.File
		var mnt lustre.RootDir

		BeforeEach(func() {
			mnt, err := lustre.MountRoot(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			testFile, err = ioutil.TempFile(string(mnt), "test")
			Ω(err).ShouldNot(HaveOccurred())
			testFile.Close()
		})
		AfterEach(func() {
			os.Remove(testFile.Name())
		})

		Describe("LookupFid()", func() {
			It("should return a valid FID, given a valid path.", func() {
				fid, err := lustre.LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())
				Expect(fid).ToNot(BeNil())
			})

			It("should return an error, given an invalid path.", func() {
				_, err := lustre.LookupFid("/foo/bar/baz")
				Ω(err).Should(HaveOccurred())
			})
		})

		Describe("FidPathname()", func() {
			It("should return a file path, given a valid fid.", func() {
				fid, err := lustre.LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())

				name, err := lustre.FidPathname(mnt, fid.String(), 0)
				Ω(err).ShouldNot(HaveOccurred())

				Expect(name).To(Equal(path.Base(testFile.Name())))
			})
		})

		Describe("FidPathnames()", func() {
			It("should return an array of paths, given a valid fid.", func() {
				fid, err := lustre.LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())

				names, err := lustre.FidPathnames(mnt, fid.String())
				Ω(err).ShouldNot(HaveOccurred())
				Expect(names[0]).To(Equal(path.Base(testFile.Name())))
			})
		})
	})

	Describe("parsing functions,", func() {
		Describe("ParseFid()", func() {
			It("should correctly parse a valid fid string.", func() {
				seq := uint64(0x123)
				oid := uint32(0x456)
				ver := uint32(0)
				str := fmt.Sprintf("[%#x:%#x:%#x]", seq, oid, ver)
				fid, err := lustre.ParseFid(str)
				Ω(err).ShouldNot(HaveOccurred())

				Expect(fid.String()).To(Equal(str))
			})

			It("should correctly parse the zero FID.", func() {
				fid, err := lustre.ParseFid("[0x0:0x0:0x0]")
				Ω(err).ShouldNot(HaveOccurred())

				Expect(fid.IsZero()).To(BeTrue())
			})

			It("should correctly parse the .lustre FID.", func() {
				fid, err := lustre.ParseFid("[0x200000002:0x1:0x0]")
				Ω(err).ShouldNot(HaveOccurred())

				Expect(fid.IsDotLustre()).To(BeTrue())
			})

			It("should return an error, given a bad FID string.", func() {
				_, err := lustre.ParseFid("[0x123:0x456:bad]")
				Ω(err).Should(HaveOccurred())
			})
		})
	})
})

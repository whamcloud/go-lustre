package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "hpdd/lustre"

	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
)

var _ = Describe("In the FID Utility Library", func() {
	Describe("lookup functions,", func() {
		var testFile *os.File
		var mnt RootDir
		var fid *Fid
		var err error

		BeforeEach(func() {
			mnt, err = MountRoot(ClientMount)
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
				fid, err = LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())
				Expect(reflect.TypeOf(fid).String()).To(Equal("*lustre.Fid"))
			})

			It("should return an error, given an invalid path.", func() {
				_, err := LookupFid("/foo/bar/baz")
				Ω(err).Should(HaveOccurred())
			})
		})

		Describe("FidPathname()", func() {
			It("should return a file path, given a valid fid.", func() {
				fid, err = LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())

				name, err := FidPathname(mnt, fid.String(), 0)
				Ω(err).ShouldNot(HaveOccurred())

				Expect(name).To(Equal(path.Base(testFile.Name())))
			})
		})

		Describe("FidPathnames()", func() {
			It("should return an array of paths, given a valid fid.", func() {
				fid, err = LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())

				names, err := FidPathnames(mnt, fid.String())
				Ω(err).ShouldNot(HaveOccurred())
				Expect(names[0]).To(Equal(path.Base(testFile.Name())))
			})
		})
	})

	Describe("parsing functions,", func() {
		Describe("ParseFid()", func() {
			It("should correctly parse a valid fid string.", func() {
				seq := 0x123
				oid := 0x456
				ver := 0
				str := fmt.Sprintf("[%#x:%#x:%#x]", seq, oid, ver)
				fid, err := ParseFid(str)
				Ω(err).ShouldNot(HaveOccurred())

				// Can't access these fields because they're
				// not exported from the lustre package.
				/*Expect(fid.f_seq).To(Equal(seq))
				Expect(fid.f_oid).To(Equal(oid))
				Expect(fid.f_ver).To(Equal(ver))*/
				Expect(fid.String()).To(Equal(str))
			})

			It("should correctly parse the zero FID.", func() {
				fid, err := ParseFid("[0x0:0x0:0x0]")
				Ω(err).ShouldNot(HaveOccurred())

				Expect(fid.IsZero()).To(BeTrue())
			})

			It("should return an error, given a bad FID string.", func() {
				_, err := ParseFid("[0x123:0x456:bad]")
				Ω(err).Should(HaveOccurred())
			})
		})
	})
})

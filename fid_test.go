// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package lustre_test

import (
	"encoding/json"

	"github.com/intel-hpdd/go-lustre"
	"github.com/intel-hpdd/go-lustre/fs"
	"github.com/intel-hpdd/go-lustre/status"
	"github.com/intel-hpdd/test/harness"

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
		var mnt fs.RootDir

		BeforeEach(func() {
			mnt, err := fs.MountRoot(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			testFile, err = ioutil.TempFile(mnt.Path(), "test")
			Ω(err).ShouldNot(HaveOccurred())
			testFile.Close()
		})
		AfterEach(func() {
			Ω(os.Remove(testFile.Name())).Should(Succeed())
		})

		Describe("LookupFid()", func() {
			It("should return a valid FID, given a valid path.", func() {
				Expect(fs.LookupFid(testFile.Name())).ToNot(BeNil())
			})

			It("should return an error, given an invalid path.", func() {
				_, err := fs.LookupFid("/foo/bar/baz")
				Ω(err).Should(HaveOccurred())
			})
		})

		Describe("FidPathname()", func() {
			It("should return a file path, given a valid fid.", func() {
				fid, err := fs.LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())

				name, err := status.FidPathname(mnt, fid, 0)
				Ω(err).ShouldNot(HaveOccurred())

				Expect(name).To(Equal(path.Base(testFile.Name())))
			})
		})

		Describe("FidPathnames()", func() {
			It("should return an array of paths, given a valid fid.", func() {
				fid, err := fs.LookupFid(testFile.Name())
				Ω(err).ShouldNot(HaveOccurred())

				names, err := status.FidPathnames(mnt, fid)
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

	Describe("marshal functions,", func() {
		Describe("MarshalLJSON()", func() {
			var fid *lustre.Fid
			var fidStr string
			BeforeEach(func() {
				seq := uint64(0x123)
				oid := uint32(0x456)
				ver := uint32(0)
				fidStr = fmt.Sprintf("[%#x:%#x:%#x]", seq, oid, ver)
				var err error
				fid, err = lustre.ParseFid(fidStr)
				Ω(err).ShouldNot(HaveOccurred())
			})

			It("should correctly marshal a fid.", func() {
				buf, err := json.Marshal(fid)
				Ω(err).ShouldNot(HaveOccurred())

				Expect(string(buf)).To(Equal(fmt.Sprintf("\"%s\"", fidStr)))
			})

			It("should correctly unmarshal a fid.", func() {
				buf, err := json.Marshal(fid)
				Ω(err).ShouldNot(HaveOccurred())

				var fid2 lustre.Fid
				err = json.Unmarshal(buf, &fid2)
				Ω(err).ShouldNot(HaveOccurred())
				Expect(&fid2).To(Equal(fid))
			})
		})
	})
})

package layout_test

import (
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/lustre/llapi/layout"
	"github.intel.com/hpdd/test/harness"
	"github.intel.com/hpdd/test/utils"
)

func createFile(name string, count, size, index int) error {
	command := exec.Command("lfs", "setstripe",
		"--stripe-count", strconv.Itoa(count),
		"--stripe-size", strconv.Itoa(size),
		"--stripe-index", strconv.Itoa(index),
		name)
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Ω(err).ShouldNot(HaveOccurred())
	Eventually(session).Should(gexec.Exit(0))
	return nil
}

var _ = Describe("In the layout library", func() {
	Describe("lookup functions,", func() {
		var testFile *os.File
		//		var mnt fs.RootDir

		BeforeEach(func() {
			mnt, err := fs.MountRoot(harness.ClientMount())
			Ω(err).ShouldNot(HaveOccurred())

			testFile, err = ioutil.TempFile(string(mnt), "test")
			Ω(err).ShouldNot(HaveOccurred())
			testFile.Close()
		})
		AfterEach(func() {
			Ω(os.Remove(testFile.Name())).Should(Succeed())
		})

		Describe("Create New Layout()", func() {
			It("should return non nil.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				l.Free()
			})

			It("should return an error, given an invalid path.", func() {
				_, err := fs.LookupFid("/foo/bar/baz")
				Ω(err).Should(HaveOccurred())
			})
		})

		Describe("Can set striping parameters on a layout", func() {
			It("Set stripe count to 5.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				Expect(l.StripeCountSet(5)).To(Succeed())
				Expect(l.StripeCount()).To(BeEquivalentTo(5))
			})

			It("Fails with invalid stripe count to.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				Expect(l.StripeCountSet(100000)).NotTo(Succeed())
			})

			It("Set stripe size to 64k.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				Expect(l.StripeSizeSet(64 * 1024)).To(Succeed())
				Expect(l.StripeSize()).To(BeEquivalentTo(64 * 1024))
			})

			It("Fails with small stripe size.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				Expect(l.StripeSizeSet(512)).NotTo(Succeed())
			})

			It("Fails with unaligned stripe size.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				Expect(l.StripeSizeSet((64 * 1024) + 1)).NotTo(Succeed())
			})

			It("Set stripe 0 index to ost 100.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				Expect(l.OstIndexSet(0, 100)).To(Succeed())
				// TODO: No way to fetch this currently
				// Expect(l.OstIndex(0)).To(BeEquivalentTo(100))
			})

			It("Set stripe pattern to DEFAULT.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				//TODO: LU-6589 this shoudln't fail, so shoudn't be NotTo
				Expect(l.PatternSet(layout.DEFAULT)).NotTo(Succeed())
				// Expect(l.Pattern()).To(Equal(layout.DEFAULT))
			})

			It("Set stripe pattern to RAID0.", func() {
				l := layout.New()
				Expect(l).ToNot(BeNil())
				defer l.Free()

				//TODO: LU-6589 this shoudln't fail, so shoudn't be NotTo
				Expect(l.PatternSet(layout.RAID0)).NotTo(Succeed())
				// Expect(l.Pattern()).To(Equal(layout.RAID0))
			})
		})
		Describe("Can read striping parameters from a file", func() {
			testFile := utils.TestFilePath("foo")
			stripeCount := 2
			stripeSize := 1 << 16
			ostIndex := 1

			BeforeEach(func() {
				Expect(createFile(testFile, stripeCount, stripeSize, ostIndex)).To(Succeed())
			})

			AfterEach(func() {
				Expect(os.Remove(testFile)).To(Succeed())
			})

			It("retrieves stripe count", func() {
				l, err := layout.GetByPath(testFile)
				Expect(err).To(Succeed())

				Expect(l.StripeCount()).To(BeEquivalentTo(stripeCount))
			})

			It("retrieves stripe size", func() {
				l, err := layout.GetByPath(testFile)
				Expect(err).To(Succeed())

				Expect(l.StripeSize()).To(BeEquivalentTo(stripeSize))
			})

			It("retrieves stripe index", func() {
				l, err := layout.GetByPath(testFile)
				Expect(err).To(Succeed())

				Expect(l.OstIndex(0)).To(BeEquivalentTo(ostIndex))
			})

			It("retrieves pattern", func() {
				l, err := layout.GetByPath(testFile)
				Expect(err).To(Succeed())

				Expect(l.Pattern()).To(BeEquivalentTo(layout.RAID0))
			})

			It("returns error if file doesn't exist", func() {
				_, err := layout.GetByPath(testFile + "badfile")
				Expect(err).NotTo(Succeed())
			})

			It("returns error if file is not on Lustre", func() {
				_, err := layout.GetByPath("/etc/motd")
				Expect(err).NotTo(Succeed())
			})
		})

		Describe("Can create file with custom layout", func() {
			testFile := utils.TestFilePath("foo")
			var stripeCount uint64 = 2
			var stripeSize uint64 = 1 << 16
			var ostIndex uint64 = 1
			var l *layout.Layout

			BeforeEach(func() {
				l = layout.New()
				l.StripeCountSet(stripeCount)
				l.StripeSizeSet(stripeSize)
				l.OstIndexSet(0, ostIndex)
			})
			AfterEach(func() {
				l.Free()
			})

			It("Create create file with open", func() {
				_, err := l.FileOpen(testFile, 0, 0775)
				Expect(err).To(Succeed())
			})

		})

	})
})

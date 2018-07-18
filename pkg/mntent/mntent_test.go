// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package mntent

import "testing"

var testFstab = `

# sample fstab

/dev/mapper/VolGroup00-LogVol00 /                       ext4    defaults        1 1
UUID=7ac5bec6-a098-4a06-9b2f-a940243b673c /boot                   ext4    defaults        1 2
/dev/mapper/VolGroup00-LogVol01 swap                    swap    defaults        0 0
10.0.2.15@tcp:/lustre /mnt/lustre                       lustre  rw,flock,user_xattr  0 0
`

var testMtab = `
rootfs / rootfs rw 0 0
sysfs /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0
proc /proc proc rw,nosuid,nodev,noexec,relatime 0 0
devtmpfs /dev devtmpfs rw,nosuid,size=495776k,nr_inodes=123944,mode=755 0 0
securityfs /sys/kernel/security securityfs rw,nosuid,nodev,noexec,relatime 0 0
tmpfs /dev/shm tmpfs rw,nosuid,nodev 0 0
devpts /dev/pts devpts rw,nosuid,noexec,relatime,gid=5,mode=620,ptmxmode=000 0 0
tmpfs /run tmpfs rw,nosuid,nodev,mode=755 0 0
tmpfs /sys/fs/cgroup tmpfs ro,nosuid,nodev,noexec,mode=755 0 0
/dev/mapper/VolGroup00-LogVol00 / ext4 rw,relatime,data=ordered 0 0
systemd-1 /proc/sys/fs/binfmt_misc autofs rw,relatime,fd=34,pgrp=1,timeout=300,minproto=5,maxproto=5,direct 0 0
mqueue /dev/mqueue mqueue rw,relatime 0 0
hugetlbfs /dev/hugepages hugetlbfs rw,relatime 0 0
debugfs /sys/kernel/debug debugfs rw,relatime 0 0
/dev/sda2 /boot ext4 rw,relatime,data=ordered 0 0
tmpfs /run/user/1000 tmpfs rw,nosuid,nodev,relatime,size=101676k,mode=700,uid=1000,gid=1000 0 0
10.0.2.15@tcp:/lustre /mnt/lustre lustre rw,flock,user_xattr 0 0
10.0.2.15@tcp0:/lustre /var/lib/lhsmd/roots/lhsm-plugin-posix lustre rw,user_xattr 0 0
10.0.2.15@tcp0:/lustre /var/lib/lhsmd/roots/lhsm-plugin-s3 lustre rw,user_xattr 0 0
`

func TestGetByDir(t *testing.T) {
	cases := []struct{ tab, dir, name string }{
		{testFstab, "/", "/dev/mapper/VolGroup00-LogVol00"},
		{testFstab, "/mnt/lustre", "10.0.2.15@tcp:/lustre"},
		{testFstab, "swap", "/dev/mapper/VolGroup00-LogVol01"},
		{testMtab, "/var/lib/lhsmd/roots/lhsm-plugin-s3", "10.0.2.15@tcp0:/lustre"},
	}
	for _, test := range cases {
		entries, err := TestEntries(test.tab)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			t.Fatalf("no entries found")
		}
		e, err := entries.ByDir(test.dir)
		if err != nil {
			t.Fatal(err)
		}
		if e.Fsname != test.name {
			t.Fatalf("wrong fsname %s, expected %s", e.Fsname, test.name)
		}

	}

}

func TestGetByType(t *testing.T) {
	cases := []struct {
		tab    string
		fsType string
		names  []string
	}{
		{testFstab, "ext4", []string{"/dev/mapper/VolGroup00-LogVol00", "UUID=7ac5bec6-a098-4a06-9b2f-a940243b673c"}},
		{testMtab, "lustre", []string{"10.0.2.15@tcp:/lustre", "10.0.2.15@tcp0:/lustre", "10.0.2.15@tcp0:/lustre"}},
	}
	for _, test := range cases {
		entries, err := TestEntries(test.tab)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			t.Fatalf("no entries found")
		}
		results, err := entries.ByType(test.fsType)
		if err != nil {
			t.Fatal(err)
		}
		for i, e := range results {
			if e.Fsname != test.names[i] {
				t.Fatalf("%s: wrong fsname %q, expected %q", test.fsType, e.Fsname, test.names[i])
			}
		}

	}
}

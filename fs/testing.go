package fs

func TestID(name string) ID {
	return ID(RootDir{path: name})
}

package command

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func tryLockGeneration(file *os.File) error {
	var offset windows.Overlapped

	err := windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &offset)
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
		return errors.New("another process is generating migrations")
	}

	return err
}

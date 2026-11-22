//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package command

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func tryLockGeneration(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) {
		return errors.New("another process is generating migrations")
	}

	return err
}

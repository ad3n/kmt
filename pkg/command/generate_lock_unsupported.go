//go:build !(darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris || windows)

package command

import (
	"errors"
	"os"
)

func tryLockGeneration(*os.File) error {
	return errors.New("generation locking is unsupported on this platform")
}

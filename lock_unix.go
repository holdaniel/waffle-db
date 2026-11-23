//go:build unix

package waffle

import (
	"errors"
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking advisory lock on f. flock locks
// belong to the open file description, so a second Open of the same path,
// in this process or another, fails with ErrLocked. The lock is released
// when f is closed.
func lockFile(f *os.File) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.EWOULDBLOCK):
			return ErrLocked
		default:
			return err
		}
	}
}

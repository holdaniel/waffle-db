//go:build !unix

package waffle

import "os"

// lockFile is a no-op on platforms without flock (e.g. Windows); there,
// callers must ensure a database file is opened by only one DB at a time.
// TODO: use LockFileEx on Windows.
func lockFile(*os.File) error { return nil }

//go:build !linux && !freebsd && !netbsd && !openbsd && !darwin

package wal

import (
	"os"
)

// fdatasync falls back to fsync on platforms without fdatasync
func fdatasync(file *os.File) error {
	// Fallback to standard fsync on other platforms
	return file.Sync()
}

// getDSyncFlag returns 0 on platforms without O_DSYNC support
func getDSyncFlag() int {
	return 0
}

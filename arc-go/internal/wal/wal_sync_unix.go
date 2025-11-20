//go:build linux || freebsd || netbsd || openbsd

package wal

import (
	"os"
	"syscall"
)

// fdatasync syncs file data to disk without syncing metadata (faster than fsync)
func fdatasync(file *os.File) error {
	// Use fdatasync syscall on Linux/Unix systems
	return syscall.Fdatasync(int(file.Fd()))
}

// getDSyncFlag returns the O_DSYNC flag for this platform
func getDSyncFlag() int {
	return syscall.O_DSYNC
}

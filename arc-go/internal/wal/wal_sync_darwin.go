//go:build darwin

package wal

import (
	"os"
	"syscall"
	"unsafe"
)

// fdatasync falls back to F_FULLFSYNC on macOS (which is like fdatasync)
// macOS doesn't have a true fdatasync syscall, but F_FULLFSYNC is similar
func fdatasync(file *os.File) error {
	// macOS: Use F_FULLFSYNC for data-only sync (similar to fdatasync)
	// This is what SQLite and other databases use on macOS
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, file.Fd(), uintptr(syscall.F_FULLFSYNC), 0)
	if errno != 0 {
		// Fallback to standard fsync if F_FULLFSYNC fails
		return file.Sync()
	}
	return nil
}

// getDSyncFlag returns 0 on macOS since O_DSYNC is not well-supported
// F_FULLFSYNC is used instead via fcntl
func getDSyncFlag() int {
	// macOS doesn't properly support O_DSYNC, we use F_FULLFSYNC in fdatasync instead
	return 0
}

var _ = unsafe.Pointer(nil) // for build constraint

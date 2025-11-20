package storage

import "errors"

// Common storage errors
var (
	ErrObjectNotFound     = errors.New("object not found")
	ErrInvalidKey         = errors.New("invalid object key")
	ErrAccessDenied       = errors.New("access denied")
	ErrQuotaExceeded      = errors.New("storage quota exceeded")
	ErrBackendUnavailable = errors.New("storage backend unavailable")
	ErrInvalidConfig      = errors.New("invalid storage configuration")
	ErrOperationTimeout   = errors.New("operation timeout")
	ErrConnectionFailed   = errors.New("connection failed")
	ErrAuthentication     = errors.New("authentication failed")
	ErrPermissionDenied   = errors.New("permission denied")
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrInvalidRange       = errors.New("invalid byte range")
)

package mqttcredential

import (
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// Auth directory and all ancestors must be real directories, not symlinks.
// The volume is private to the shared trusted UID; malicious same-UID writers
// are outside the contract. O_NOFOLLOW also protects each opened file.
func secureDir(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return ErrInvalidInput
	}
	for p := path; ; p = filepath.Dir(p) {
		st, err := os.Lstat(p)
		if err != nil || !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
			return ErrPersistenceFailure
		}
		if p == "/" {
			break
		}
	}
	st, err := os.Stat(path)
	if err != nil || st.Mode().Perm() != 0700 || !ownerIdentity(st) {
		return ErrPersistenceFailure
	}
	return nil
}

func owned(st os.FileInfo) bool {
	s, ok := st.Sys().(*syscall.Stat_t)
	return ok && s.Uid == uint32(os.Geteuid()) && s.Gid == uint32(os.Getegid()) && s.Nlink == 1
}

func ownerIdentity(st os.FileInfo) bool {
	s, ok := st.Sys().(*syscall.Stat_t)
	return ok && s.Uid == uint32(os.Geteuid()) && s.Gid == uint32(os.Getegid())
}

func openSecure(path string, flags int) (*os.File, error) {
	fd, err := syscall.Open(path, flags|syscall.O_NOFOLLOW|syscall.O_CLOEXEC|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, ErrPersistenceFailure
	}
	f := os.NewFile(uintptr(fd), path)
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || !owned(st) {
		_ = f.Close()
		return nil, ErrPersistenceFailure
	}
	return f, nil
}

func readSecure(path string, limit int64) ([]byte, error) {
	f, err := openSecure(path, syscall.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil || int64(len(b)) > limit {
		return nil, ErrPersistenceFailure
	}
	return b, nil
}

// fileOps is private fault-injection plumbing, never an alternate durability mode.
type fileOps struct {
	syncFile func(*os.File) error
	rename   func(string, string) error
	syncDir  func(string) error
}

func defaultFileOps() fileOps { return fileOps{(*os.File).Sync, os.Rename, syncDirectory} }
func syncDirectory(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}
func writeSnapshot(path string, b []byte, ops fileOps) error {
	f, err := openSecure(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = ops.syncFile(f)
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return ErrPersistenceFailure
	}
	return nil
}

func syncSecure(path string, ops fileOps) error {
	f, err := openSecure(path, syscall.O_RDWR)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if ops.syncFile(f) != nil {
		return ErrPersistenceFailure
	}
	return nil
}

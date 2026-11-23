package waffle

import (
	"io"
	"os"
	"sync"
)

// storage is the byte container behind a DB: an *os.File or a memStorage.
type storage interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Truncate(size int64) error
	Close() error
}

func storageSize(s storage) (int64, error) {
	switch f := s.(type) {
	case *os.File:
		fi, err := f.Stat()
		if err != nil {
			return 0, err
		}
		return fi.Size(), nil
	case *memStorage:
		f.mu.RLock()
		defer f.mu.RUnlock()
		return int64(len(f.b)), nil
	}
	panic("waffle: unknown storage")
}

type memStorage struct {
	mu sync.RWMutex
	b  []byte
}

func (m *memStorage) ReadAt(p []byte, off int64) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if off >= int64(len(m.b)) {
		return 0, io.EOF
	}
	n := copy(p, m.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *memStorage) WriteAt(p []byte, off int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	end := int(off) + len(p)
	if end > len(m.b) {
		m.b = grow(m.b, end)
	}
	return copy(m.b[off:], p), nil
}

func (m *memStorage) Truncate(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if int(size) < len(m.b) {
		m.b = m.b[:size]
	}
	return nil
}

func (m *memStorage) Sync() error  { return nil }
func (m *memStorage) Close() error { return nil }

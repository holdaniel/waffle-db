package waffle

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// On-disk layout
//
//	file   := header frame*
//	header := "WAFLDB01"                      (8 bytes)
//	frame  := len:u32le crc:u32le payload     (crc32-Castagnoli of payload)
//	payload:= op*
//	op     := 0x01 klen:uvarint key vlen:uvarint value   (put)
//	        | 0x02 klen:uvarint key                      (delete)
//
// Every commit (a single Put or a whole Update transaction) is exactly one
// frame, so commits are atomic: on open, the log is replayed and anything
// after the first torn or corrupt frame is truncated away.

const (
	magic     = "WAFLDB01"
	headerLen = int64(len(magic))
	frameHdr  = 8
	opPut     = 0x01
	opDel     = 0x02
	maxFrame  = 1<<31 - 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrClosed   = errors.New("waffle: database is closed")
	ErrTxDone   = errors.New("waffle: transaction has already finished")
	ErrTooLarge = errors.New("waffle: commit exceeds 2 GiB frame limit")
)

// Options configures Open.
type Options struct {
	// SyncWrites fsyncs the log after every commit. Without it, commits reach
	// the OS page cache only; a power loss may drop the most recent commits
	// but never corrupts or partially applies one.
	SyncWrites bool
}

// DB is a single-file, ordered, transactional key-value store. Use it through
// typed Buckets; see NewBucket.
//
// Concurrency: any number of goroutines may read while one writer commits.
// Writes (Put, Delete, Update) are serialised.
type DB struct {
	mu  sync.RWMutex // guards idx, f, size, closed for readers
	wmu sync.Mutex   // serialises writers (and compaction)

	idx       *index
	f         storage
	size      int64
	path      string // "" for in-memory
	opts      Options
	closed    bool
	truncated int64

	frame []byte // writer-owned scratch buffer
}

// Open opens (creating if needed) the database file at path.
func Open(path string, opts *Options) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	db := &DB{f: f, path: path, idx: newIndex()}
	if opts != nil {
		db.opts = *opts
	}
	if err := db.load(); err != nil {
		f.Close()
		return nil, fmt.Errorf("waffle: open %s: %w", path, err)
	}
	return db, nil
}

// OpenMemory returns a database that lives entirely in memory. It behaves
// exactly like a file-backed one (same encoding, transactions, compaction),
// which makes it handy for tests and caches.
func OpenMemory() *DB {
	db := &DB{f: &memStorage{}, idx: newIndex()}
	if err := db.load(); err != nil {
		panic(err) // cannot happen for an empty memStorage
	}
	return db
}

func (db *DB) load() error {
	size, err := storageSize(db.f)
	if err != nil {
		return err
	}
	if size < headerLen {
		// New (or crashed while being created): write a fresh header.
		if err := db.f.Truncate(0); err != nil {
			return err
		}
		if _, err := db.f.WriteAt([]byte(magic), 0); err != nil {
			return err
		}
		db.size = headerLen
		return nil
	}
	var hdr [headerLen]byte
	if _, err := db.f.ReadAt(hdr[:], 0); err != nil {
		return err
	}
	if string(hdr[:]) != magic {
		return errors.New("not a waffle database (bad magic)")
	}
	good, err := replay(db.f, size, db.idx)
	if err != nil {
		return err
	}
	if good < size {
		db.truncated = size - good
		if err := db.f.Truncate(good); err != nil {
			return err
		}
	}
	db.size = good
	return nil
}

// replay applies every intact frame to idx and returns the offset just past
// the last intact frame.
func replay(f storage, size int64, idx *index) (int64, error) {
	r := bufio.NewReaderSize(io.NewSectionReader(f, headerLen, size-headerLen), 1<<20)
	off := headerLen
	var hdr [frameHdr]byte
	var payload []byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return off, nil // clean EOF or torn header
		}
		n := int64(binary.LittleEndian.Uint32(hdr[0:]))
		sum := binary.LittleEndian.Uint32(hdr[4:])
		if n > size-off-frameHdr {
			return off, nil // torn frame
		}
		payload = grow(payload[:0], int(n))
		if _, err := io.ReadFull(r, payload); err != nil {
			return off, nil
		}
		if crc32.Checksum(payload, castagnoli) != sum {
			return off, nil
		}
		base := off + frameHdr
		err := walkOps(payload, func(del bool, kpos, klen, vpos, vlen int) {
			key := payload[kpos : kpos+klen]
			if del {
				idx.delete(key)
			} else {
				idx.set(key, base+int64(vpos), uint32(vlen))
			}
		})
		if err != nil {
			return off, nil // checksum matched but ops are malformed: stop here
		}
		off = base + n
	}
}

// walkOps parses a frame payload, reporting byte positions relative to it.
func walkOps(p []byte, fn func(del bool, kpos, klen, vpos, vlen int)) error {
	pos := 0
	for pos < len(p) {
		op := p[pos]
		pos++
		klen, n := binary.Uvarint(p[pos:])
		if n <= 0 || klen > uint64(len(p)-pos-n) {
			return ErrCorrupt
		}
		pos += n
		kpos := pos
		pos += int(klen)
		switch op {
		case opDel:
			fn(true, kpos, int(klen), 0, 0)
		case opPut:
			vlen, n := binary.Uvarint(p[pos:])
			if n <= 0 || vlen > uint64(len(p)-pos-n) {
				return ErrCorrupt
			}
			pos += n
			fn(false, kpos, int(klen), pos, int(vlen))
			pos += int(vlen)
		default:
			return ErrCorrupt
		}
	}
	return nil
}

type writeOp struct {
	key, val []byte
	del      bool
}

// commitLocked writes ops as one atomic frame and applies them to the index.
// Caller must hold db.wmu.
func (db *DB) commitLocked(ops []writeOp) error {
	if db.closed {
		return ErrClosed
	}
	buf := append(db.frame[:0], make([]byte, frameHdr)...)
	wrote := false
	for _, op := range ops {
		if op.del {
			if db.idx.get(op.key) == nil {
				continue // no tombstone needed; idx is stable while we hold wmu
			}
			buf = append(buf, opDel)
			buf = binary.AppendUvarint(buf, uint64(len(op.key)))
			buf = append(buf, op.key...)
		} else {
			buf = append(buf, opPut)
			buf = binary.AppendUvarint(buf, uint64(len(op.key)))
			buf = append(buf, op.key...)
			buf = binary.AppendUvarint(buf, uint64(len(op.val)))
			buf = append(buf, op.val...)
		}
		wrote = true
	}
	db.frame = buf
	if !wrote {
		return nil
	}
	payload := buf[frameHdr:]
	if len(payload) > maxFrame {
		return ErrTooLarge
	}
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:], crc32.Checksum(payload, castagnoli))

	if _, err := db.f.WriteAt(buf, db.size); err != nil {
		_ = db.f.Truncate(db.size) // drop any partial write
		return err
	}
	if db.opts.SyncWrites {
		if err := db.f.Sync(); err != nil {
			return err
		}
	}

	base := db.size + frameHdr
	db.mu.Lock()
	_ = walkOps(payload, func(del bool, kpos, klen, vpos, vlen int) {
		key := payload[kpos : kpos+klen]
		if del {
			db.idx.delete(key)
		} else {
			db.idx.set(key, base+int64(vpos), uint32(vlen))
		}
	})
	db.size += int64(len(buf))
	db.mu.Unlock()

	if cap(db.frame) > 4<<20 {
		db.frame = nil // don't pin a huge buffer after a big transaction
	}
	return nil
}

// --- store implementation (non-transactional, auto-commit) ---

func (db *DB) get(key, dst []byte) ([]byte, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return dst, false, ErrClosed
	}
	n := db.idx.get(key)
	if n == nil {
		return dst, false, nil
	}
	dst = grow(dst[:0], int(n.vlen))
	if err := readFull(db.f, dst, n.off); err != nil {
		return dst, false, err
	}
	return dst, true, nil
}

func (db *DB) put(key, val []byte) error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	ops := [1]writeOp{{key: key, val: val}}
	return db.commitLocked(ops[:])
}

func (db *DB) del(key []byte) error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	ops := [1]writeOp{{key: key, del: true}}
	return db.commitLocked(ops[:])
}

const scanChunk = 128

// scan calls fn for each key in [lo, hi) in order (hi == nil: unbounded).
// The k and v slices are only valid during the call.
//
// Entries are read in chunks under the read lock and handed to fn with the
// lock released, so fn may freely write to the database. The scan is
// therefore not a point-in-time snapshot: each entry is a committed value,
// but writes made during the scan may or may not be observed.
func (db *DB) scan(lo, hi []byte, keysOnly bool, fn func(k, v []byte) bool) error {
	var (
		cur       = bytes.Clone(lo)
		inclusive = true
		arena     []byte
		spans     []int // k0start, k0end, v0end, k1start, ...
	)
	for {
		arena, spans = arena[:0], spans[:0]
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return ErrClosed
		}
		n := db.idx.seek(cur, nil)
		if !inclusive && n != nil && bytes.Equal(n.key, cur) {
			n = n.next[0]
		}
		count := 0
		for ; n != nil && count < scanChunk; n = n.next[0] {
			if hi != nil && bytes.Compare(n.key, hi) >= 0 {
				break
			}
			ks := len(arena)
			arena = append(arena, n.key...)
			ke := len(arena)
			if !keysOnly {
				arena = grow(arena, ke+int(n.vlen))
				if err := readFull(db.f, arena[ke:], n.off); err != nil {
					db.mu.RUnlock()
					return err
				}
			}
			spans = append(spans, ks, ke, len(arena))
			count++
		}
		more := n != nil && count == scanChunk
		db.mu.RUnlock()

		for i := 0; i < len(spans); i += 3 {
			if !fn(arena[spans[i]:spans[i+1]], arena[spans[i+1]:spans[i+2]]) {
				return nil
			}
		}
		if !more {
			return nil
		}
		last := len(spans) - 3
		cur = append(cur[:0], arena[spans[last]:spans[last+1]]...)
		inclusive = false
	}
}

// Update runs fn inside a read-write transaction. All writes made through
// the Tx (see Bucket.In) become visible and durable atomically when fn
// returns nil; if fn returns an error or panics, nothing is written.
//
// Writers are serialised, so do not call non-transactional write methods
// (Bucket.Put on a Bucket not bound with In) from inside fn: they would
// deadlock waiting for this transaction.
func (db *DB) Update(fn func(tx *Tx) error) error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed {
		return ErrClosed
	}
	tx := &Tx{db: db, writes: make(map[string]txWrite)}
	defer func() { tx.done = true }()
	if err := fn(tx); err != nil {
		return err
	}
	tx.done = true
	return db.commitLocked(tx.ops())
}

// Stats describes the database's size and garbage.
type Stats struct {
	Keys      int   // live keys across all buckets
	FileBytes int64 // current log size
	LiveBytes int64 // approximate bytes needed to store only live data
	// TruncatedBytes is how much of a torn tail was discarded on Open.
	TruncatedBytes int64
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return Stats{
		Keys:           db.idx.n,
		FileBytes:      db.size,
		LiveBytes:      headerLen + db.idx.live,
		TruncatedBytes: db.truncated,
	}
}

// Compact rewrites the log keeping only live values, reclaiming the space
// used by overwritten and deleted entries. Readers keep working during
// compaction; writers wait.
func (db *DB) Compact() (err error) {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed {
		return ErrClosed
	}

	var dst storage
	tmp := ""
	if db.path == "" {
		dst = &memStorage{}
	} else {
		tmp = db.path + ".compact"
		f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		dst = f
		defer func() {
			if err != nil {
				dst.Close()
				os.Remove(tmp)
			}
		}()
	}

	newIdx := newIndex()
	if _, err := dst.WriteAt([]byte(magic), 0); err != nil {
		return err
	}
	off := headerLen
	buf := make([]byte, frameHdr, 1<<20)
	type pending struct {
		kpos, klen, vpos int
		vlen             uint32
	}
	var ents []pending

	flush := func() error {
		if len(buf) == frameHdr {
			return nil
		}
		payload := buf[frameHdr:]
		binary.LittleEndian.PutUint32(buf[0:], uint32(len(payload)))
		binary.LittleEndian.PutUint32(buf[4:], crc32.Checksum(payload, castagnoli))
		if _, err := dst.WriteAt(buf, off); err != nil {
			return err
		}
		for _, e := range ents {
			newIdx.set(buf[e.kpos:e.kpos+e.klen], off+int64(e.vpos), e.vlen)
		}
		off += int64(len(buf))
		buf, ents = buf[:frameHdr], ents[:0]
		return nil
	}

	// The index only changes under wmu, which we hold, so it is safe to walk
	// without db.mu; concurrent readers only read it.
	for n := db.idx.first(); n != nil; n = n.next[0] {
		buf = append(buf, opPut)
		buf = binary.AppendUvarint(buf, uint64(len(n.key)))
		kpos := len(buf)
		buf = append(buf, n.key...)
		buf = binary.AppendUvarint(buf, uint64(n.vlen))
		vpos := len(buf)
		buf = grow(buf, vpos+int(n.vlen))
		if err := readFull(db.f, buf[vpos:], n.off); err != nil {
			return err
		}
		ents = append(ents, pending{kpos, len(n.key), vpos, n.vlen})
		if len(buf) >= 4<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := dst.Sync(); err != nil {
		return err
	}
	if tmp != "" {
		if err := os.Rename(tmp, db.path); err != nil {
			return err
		}
		syncDir(filepath.Dir(db.path))
	}

	db.mu.Lock()
	old := db.f
	db.f, db.idx, db.size = dst, newIdx, off
	db.mu.Unlock()
	return old.Close()
}

// Sync flushes the log to stable storage.
func (db *DB) Sync() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	return db.f.Sync()
}

// Close syncs and closes the database. It waits for an in-flight writer.
func (db *DB) Close() error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	db.closed = true
	err := db.f.Sync()
	if cerr := db.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// --- helpers ---

func grow(b []byte, n int) []byte {
	if cap(b) >= n {
		return b[:n]
	}
	nb := make([]byte, n, max(n, 2*cap(b)))
	copy(nb, b)
	return nb
}

func readFull(r io.ReaderAt, b []byte, off int64) error {
	n, err := r.ReadAt(b, off)
	if n == len(b) {
		return nil
	}
	if err == nil || err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return err
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
}

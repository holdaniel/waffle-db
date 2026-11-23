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
//	header := "WAFLDB01"                                  (8 bytes)
//	frame  := len:u32le pcrc:u32le hcrc:u32le payload
//	          pcrc = crc32c(payload), hcrc = crc32c(len ‖ pcrc), len > 0
//	payload:= op*
//	op     := 0x01 klen:uvarint key vlen:uvarint value     (put)
//	        | 0x02 klen:uvarint key                        (delete)
//	        | 0x03 klen:uvarint lo  hlen:uvarint hi        (delete [lo, hi); hlen 0 = unbounded)
//
// Every commit (a single Put or a whole Update transaction) is exactly one
// frame, so commits are atomic. On open the log is replayed. A damaged frame
// with no intact frame after it is a torn tail from a crash and is truncated.
// A damaged frame followed by intact frames is real corruption: Open refuses
// with a *CorruptionError rather than silently discarding the later commits.
// The header checksum (hcrc) is what makes that distinction possible: it lets
// recovery recognise frame boundaries even when a length field is damaged.

const (
	magic      = "WAFLDB01"
	headerLen  = int64(len(magic))
	frameHdr   = 12
	opPut      = 0x01
	opDel      = 0x02
	opDelRange = 0x03
	maxFrame   = 1<<31 - 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrClosed   = errors.New("waffle: database is closed")
	ErrTxDone   = errors.New("waffle: transaction has already finished")
	ErrTooLarge = errors.New("waffle: commit exceeds 2 GiB frame limit")
	ErrLocked   = errors.New("waffle: database file is locked by another handle or process")
	// ErrFailed is returned by every write after a write or fsync failed in a
	// way that leaves the on-disk state uncertain. Reads keep working; reopen
	// the database to recover.
	ErrFailed = errors.New("waffle: database is read-only after a failed write; reopen it")
)

// CorruptionError reports a damaged frame in the middle of the log, i.e.
// one that has intact frames after it. It matches errors.Is(err, ErrCorrupt).
type CorruptionError struct {
	Offset int64 // start of the damaged frame
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("waffle: log is corrupted at offset %d and has intact data after it; "+
		"refusing to open (set Options.TruncateCorrupt to discard everything from that offset)", e.Offset)
}

func (e *CorruptionError) Is(target error) bool { return target == ErrCorrupt }

// Options configures Open.
type Options struct {
	// SyncWrites fsyncs the log after every commit. Without it, commits reach
	// the OS page cache only; a power loss may drop the most recent commits
	// but never corrupts or partially applies one.
	SyncWrites bool

	// TruncateCorrupt makes Open discard everything from the first damaged
	// frame onward, even when intact frames follow it. Use it to salvage a
	// database that fails to open with a *CorruptionError, after taking a
	// copy of the file.
	TruncateCorrupt bool
}

// DB is a single-file, ordered, transactional key-value store. Use it through
// typed Buckets; see NewBucket.
//
// Concurrency: any number of goroutines may read while one writer commits.
// Writes (Put, Delete, Update) are serialised. A file can be open by only
// one DB at a time (enforced with an OS file lock on Unix).
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
	werr      error // sticky write failure (guarded by wmu)

	frame []byte // writer-owned scratch buffer
}

// Open opens (creating if needed) the database file at path.
func Open(path string, opts *Options) (*DB, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := lockFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("waffle: open %s: %w", path, err)
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
		// Only initialise an empty file, or one that holds a prefix of our
		// header (a crash while creating it). Never clobber a foreign file.
		if size > 0 {
			var b [headerLen]byte
			if err := readFull(db.f, b[:size], 0); err != nil {
				return err
			}
			if string(b[:size]) != magic[:size] {
				return errors.New("not a waffle database (bad magic)")
			}
		}
		if _, err := db.f.WriteAt([]byte(magic), 0); err != nil {
			return err
		}
		db.size = headerLen
		return nil
	}
	var hdr [headerLen]byte
	if err := readFull(db.f, hdr[:], 0); err != nil {
		return err
	}
	if string(hdr[:]) != magic {
		return errors.New("not a waffle database (bad magic)")
	}
	good, err := replay(db.f, size, db.idx)
	var ce *CorruptionError
	if errors.As(err, &ce) && db.opts.TruncateCorrupt {
		err = nil
	}
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
// the last one applied. If it stops at a damaged frame that has intact frames
// after it, it also returns a *CorruptionError.
func replay(f storage, size int64, idx *index) (int64, error) {
	r := bufio.NewReaderSize(io.NewSectionReader(f, headerLen, size-headerLen), 1<<20)
	off := headerLen
	var hdr [frameHdr]byte
	var payload []byte
	for off < size {
		ok := false
		if _, err := io.ReadFull(r, hdr[:]); err == nil {
			if n, pcrc, hok := parseFrameHeader(hdr[:]); hok && n <= size-off-frameHdr {
				payload = grow(payload[:0], int(n))
				if _, err := io.ReadFull(r, payload); err == nil &&
					crc32.Checksum(payload, castagnoli) == pcrc &&
					walkOps(payload, nil) == nil {
					applyOps(idx, payload, off+frameHdr)
					off += frameHdr + int64(n)
					ok = true
				}
			}
		}
		if !ok {
			found, err := validFrameAfter(f, off+1, size)
			if err != nil {
				return off, err
			}
			if found {
				return off, &CorruptionError{Offset: off}
			}
			return off, nil // torn tail
		}
	}
	return off, nil
}

func sealFrame(buf []byte) {
	payload := buf[frameHdr:]
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:], crc32.Checksum(payload, castagnoli))
	binary.LittleEndian.PutUint32(buf[8:], crc32.Checksum(buf[:8], castagnoli))
}

func parseFrameHeader(h []byte) (n int64, pcrc uint32, ok bool) {
	if crc32.Checksum(h[:8], castagnoli) != binary.LittleEndian.Uint32(h[8:]) {
		return 0, 0, false
	}
	n = int64(binary.LittleEndian.Uint32(h[0:]))
	return n, binary.LittleEndian.Uint32(h[4:]), n > 0
}

// validFrameAfter reports whether any fully intact frame starts at or after
// start. It slides over the bytes checking the 4-byte header checksum at each
// offset, and verifies the payload of any header that matches. It only runs
// when replay hits damage, so its cost is irrelevant on the normal path.
func validFrameAfter(f storage, start, size int64) (bool, error) {
	const window = 1 << 20
	buf := make([]byte, window+frameHdr-1)
	var payload []byte
	for base := start; base+frameHdr <= size; base += window {
		n := min(int64(len(buf)), size-base)
		if err := readFull(f, buf[:n], base); err != nil {
			return false, err
		}
		for i := int64(0); i+frameHdr <= n && i < window; i++ {
			fl, pcrc, ok := parseFrameHeader(buf[i : i+frameHdr])
			p := base + i
			if !ok || fl > size-p-frameHdr {
				continue
			}
			payload = grow(payload[:0], int(fl))
			if err := readFull(f, payload, p+frameHdr); err != nil {
				return false, err
			}
			if crc32.Checksum(payload, castagnoli) == pcrc && walkOps(payload, nil) == nil {
				return true, nil
			}
		}
	}
	return false, nil
}

// walkOps validates a frame payload and, if fn is non-nil, reports each op
// with byte positions relative to the payload. For opDelRange the "value"
// is the exclusive upper bound.
func walkOps(p []byte, fn func(op byte, kpos, klen, vpos, vlen int)) error {
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
			if fn != nil {
				fn(op, kpos, int(klen), 0, 0)
			}
		case opPut, opDelRange:
			vlen, n := binary.Uvarint(p[pos:])
			if n <= 0 || vlen > uint64(len(p)-pos-n) {
				return ErrCorrupt
			}
			pos += n
			if fn != nil {
				fn(op, kpos, int(klen), pos, int(vlen))
			}
			pos += int(vlen)
		default:
			return ErrCorrupt
		}
	}
	return nil
}

// applyOps applies an already-validated payload located at file offset base.
func applyOps(idx *index, payload []byte, base int64) {
	_ = walkOps(payload, func(op byte, kpos, klen, vpos, vlen int) {
		key := payload[kpos : kpos+klen]
		switch op {
		case opPut:
			idx.set(key, base+int64(vpos), uint32(vlen))
		case opDel:
			idx.delete(key)
		case opDelRange:
			var hi []byte
			if vlen > 0 {
				hi = payload[vpos : vpos+vlen]
			}
			idx.deleteRange(key, hi)
		}
	})
}

type writeOp struct {
	op       byte   // opPut, opDel or opDelRange
	key, val []byte // for opDelRange: lo, hi (nil = unbounded)
}

// commitLocked writes ops as one atomic frame and applies them to the index.
// Caller must hold db.wmu.
func (db *DB) commitLocked(ops []writeOp) error {
	if db.closed {
		return ErrClosed
	}
	if db.werr != nil {
		return db.werr
	}
	buf := append(db.frame[:0], make([]byte, frameHdr)...)
	wrote := false
	for _, op := range ops {
		// idx only changes under wmu, which we hold, so reading it is safe.
		switch op.op {
		case opDel:
			if db.idx.get(op.key) == nil {
				continue // nothing to delete: no tombstone needed
			}
		case opDelRange:
			if !db.idx.anyInRange(op.key, op.val) {
				continue
			}
		}
		buf = append(buf, op.op)
		buf = binary.AppendUvarint(buf, uint64(len(op.key)))
		buf = append(buf, op.key...)
		if op.op != opDel {
			buf = binary.AppendUvarint(buf, uint64(len(op.val)))
			buf = append(buf, op.val...)
		}
		wrote = true
	}
	db.frame = buf
	if !wrote {
		return nil
	}
	if len(buf)-frameHdr > maxFrame {
		return ErrTooLarge
	}
	sealFrame(buf)

	if _, err := db.f.WriteAt(buf, db.size); err != nil {
		return db.failWrite(err, false)
	}
	if db.opts.SyncWrites {
		if err := db.f.Sync(); err != nil {
			// After a failed fsync the kernel may have dropped the dirty pages;
			// we can no longer tell what is on disk. Remove the frame so a
			// commit we reported as failed cannot reappear after a restart,
			// and refuse further writes until the database is reopened.
			return db.failWrite(err, true)
		}
	}

	db.mu.Lock()
	applyOps(db.idx, buf[frameHdr:], db.size+frameHdr)
	db.size += int64(len(buf))
	db.mu.Unlock()

	if cap(db.frame) > 4<<20 {
		db.frame = nil // don't pin a huge buffer after a big transaction
	}
	return nil
}

// failWrite rolls the file back to the last committed size. If that is not
// possible, or poison is set, the DB rejects all further writes.
func (db *DB) failWrite(err error, poison bool) error {
	if terr := db.f.Truncate(db.size); terr != nil {
		poison = true
	}
	if poison {
		db.werr = fmt.Errorf("%w (cause: %w)", ErrFailed, err)
	}
	return err
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

func (db *DB) commit1(op writeOp) error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	ops := [1]writeOp{op}
	return db.commitLocked(ops[:])
}

func (db *DB) put(key, val []byte) error { return db.commit1(writeOp{op: opPut, key: key, val: val}) }
func (db *DB) del(key []byte) error      { return db.commit1(writeOp{op: opDel, key: key}) }
func (db *DB) delRange(lo, hi []byte) error {
	return db.commit1(writeOp{op: opDelRange, key: lo, val: hi})
}

const (
	scanChunk      = 128     // max entries read per lock acquisition
	scanChunkBytes = 1 << 20 // ...and max bytes, so large values can't blow up memory
)

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
		for n != nil {
			if hi != nil && bytes.Compare(n.key, hi) >= 0 {
				n = nil // reached the end of the range
				break
			}
			if count == scanChunk || (count > 0 && len(arena) >= scanChunkBytes) {
				break // chunk full; n is where the next chunk starts
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
			n = n.next[0]
		}
		more := n != nil
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
		if cap(arena) > 4*scanChunkBytes {
			arena = nil // one huge value: don't keep its buffer for the rest of the scan
		}
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
	if db.werr != nil {
		return db.werr
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
	// TruncatedBytes is how much of a damaged tail was discarded on Open.
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
func (db *DB) Compact() error {
	db.wmu.Lock()
	defer db.wmu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.werr != nil {
		return db.werr
	}

	var dst storage = &memStorage{}
	tmp := ""
	if db.path != "" {
		tmp = db.path + ".compact"
		f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		// Lock the new file before it replaces the old one, so the lock that
		// guards db.path never lapses.
		if err := lockFile(f); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		dst = f
	}
	abort := func(err error) error {
		dst.Close()
		if tmp != "" {
			os.Remove(tmp)
		}
		return err
	}

	newIdx, size, err := db.copyLive(dst)
	if err != nil {
		return abort(err)
	}
	if err := dst.Sync(); err != nil {
		return abort(err)
	}
	if tmp != "" {
		if err := os.Rename(tmp, db.path); err != nil {
			return abort(err)
		}
		syncDir(filepath.Dir(db.path))
	}

	// Point of no return: dst is now the database.
	db.mu.Lock()
	old := db.f
	db.f, db.idx, db.size = dst, newIdx, size
	db.mu.Unlock()
	_ = old.Close() // already unlinked/replaced; its close error can't affect data
	return nil
}

// copyLive writes every live entry to dst and returns the matching index.
// Caller holds wmu, so db.idx is stable and can be walked without db.mu.
func (db *DB) copyLive(dst storage) (*index, int64, error) {
	newIdx := newIndex()
	if _, err := dst.WriteAt([]byte(magic), 0); err != nil {
		return nil, 0, err
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
		sealFrame(buf)
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

	for n := db.idx.first(); n != nil; n = n.next[0] {
		buf = append(buf, opPut)
		buf = binary.AppendUvarint(buf, uint64(len(n.key)))
		kpos := len(buf)
		buf = append(buf, n.key...)
		buf = binary.AppendUvarint(buf, uint64(n.vlen))
		vpos := len(buf)
		buf = grow(buf, vpos+int(n.vlen))
		if err := readFull(db.f, buf[vpos:], n.off); err != nil {
			return nil, 0, err
		}
		ents = append(ents, pending{kpos, len(n.key), vpos, n.vlen})
		if len(buf) >= 4<<20 {
			if err := flush(); err != nil {
				return nil, 0, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, 0, err
	}
	return newIdx, off, nil
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

// Close syncs and closes the database, releasing the file lock. It waits for
// an in-flight writer.
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

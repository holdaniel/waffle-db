package waffle

// Regression tests for the issues found in the first code review.

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// storageWrapper injects failures into a DB's storage.
type storageWrapper struct {
	storage
	syncErr, closeErr error
}

func (w *storageWrapper) Sync() error {
	if w.syncErr != nil {
		return w.syncErr
	}
	return w.storage.Sync()
}

func (w *storageWrapper) Close() error {
	err := w.storage.Close()
	if w.closeErr != nil {
		return w.closeErr
	}
	return err
}

func writeThreeFrames(t *testing.T) string {
	t.Helper()
	db, path := openTemp(t)
	b := MustBucket[int, string](db, "x")
	for i := range 3 {
		must(t, b.Put(i, fmt.Sprint("value-", i)))
	}
	must(t, db.Close())
	return path
}

// #1: damage in the middle of the log must not silently drop later commits.
func TestMidFileCorruptionRefusesToOpen(t *testing.T) {
	for name, off := range map[string]int64{
		"payload byte": headerLen + frameHdr + 2,
		"length field": headerLen,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeThreeFrames(t)
			data, _ := os.ReadFile(path)
			data[off] ^= 0x40
			must(t, os.WriteFile(path, data, 0o644))

			_, err := Open(path, nil)
			var ce *CorruptionError
			if !errors.As(err, &ce) || !errors.Is(err, ErrCorrupt) || ce.Offset != headerLen {
				t.Fatalf("want CorruptionError at %d, got %v", headerLen, err)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(after, data) {
				t.Fatal("failed Open modified the file")
			}

			db, err := Open(path, &Options{TruncateCorrupt: true})
			must(t, err)
			defer db.Close()
			if st := db.Stats(); st.Keys != 0 || st.TruncatedBytes == 0 {
				t.Fatalf("salvage stats %+v", st)
			}
		})
	}
}

func TestZeroFilledTailIsTornNotCorrupt(t *testing.T) {
	path := writeThreeFrames(t)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write(make([]byte, 4096)) // what some filesystems leave after a crash
	f.Close()
	db, err := Open(path, nil)
	must(t, err)
	defer db.Close()
	if st := db.Stats(); st.Keys != 3 || st.TruncatedBytes != 4096 {
		t.Fatalf("stats %+v", st)
	}
}

// #2: a file may be open by only one DB at a time, including across Compact.
func TestFileLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no flock on Windows")
	}
	db, path := openTemp(t)
	if _, err := Open(path, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open: %v", err)
	}
	must(t, MustBucket[int, int](db, "b").Put(1, 1))
	must(t, db.Compact()) // file replaced by rename: the lock must carry over
	if _, err := Open(path, nil); !errors.Is(err, ErrLocked) {
		t.Fatalf("open after compact: %v", err)
	}
	must(t, db.Close())
	db2, err := Open(path, nil)
	must(t, err)
	must(t, db2.Close())
}

// #3: types whose data lives in unexported fields must not be stored as empty.
type hiddenOnly struct{ n int }

type binOnly struct{ secret string } // MarshalBinary only, no AppendBinary

func (b binOnly) MarshalBinary() ([]byte, error) { return []byte(b.secret), nil }
func (b *binOnly) UnmarshalBinary(p []byte) error {
	b.secret = string(p)
	return nil
}

type embeddedBase struct{ ID int }

type withEmbedded struct {
	embeddedBase
	Name string
}

func TestUnexportedFieldsAreNotSilentlyDropped(t *testing.T) {
	db := OpenMemory()
	if _, err := NewBucket[string, hiddenOnly](db, "h"); err == nil {
		t.Fatal("struct with only unexported fields accepted")
	}
	if _, err := NewBucket[hiddenOnly, int](db, "hk"); err == nil {
		t.Fatal("key with only unexported fields accepted")
	}
	type mixedKey struct {
		ID   int
		rank int
	}
	if _, err := NewBucket[mixedKey, int](db, "mk"); err == nil {
		t.Fatal("key with an unexported field accepted")
	}
	type skippedKey struct {
		ID    int
		cache int `waffle:"-"`
	}
	if _, err := NewBucket[skippedKey, int](db, "sk"); err != nil {
		t.Fatalf("explicitly skipped field rejected: %v", err)
	}

	bins := MustBucket[string, binOnly](db, "bin")
	must(t, bins.Put("a", binOnly{"s3cret"}))
	if v, _, _ := bins.Get("a"); v.secret != "s3cret" {
		t.Fatalf("BinaryMarshaler round trip: %+v", v)
	}

	type Account struct {
		Owner   string
		Balance *big.Int
		Limit   big.Int
	}
	accts := MustBucket[string, Account](db, "acct")
	huge, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	in := Account{"ada", huge, *big.NewInt(-42)}
	must(t, accts.Put("a", in))
	out, _, err := accts.Get("a")
	must(t, err)
	if out.Balance.Cmp(huge) != 0 || out.Limit.Int64() != -42 {
		t.Fatalf("big.Int round trip: %v %v", out.Balance, &out.Limit)
	}

	emb := MustBucket[string, withEmbedded](db, "emb")
	must(t, emb.Put("a", withEmbedded{embeddedBase{7}, "x"}))
	if v, _, _ := emb.Get("a"); v.ID != 7 || v.Name != "x" {
		t.Fatalf("embedded unexported struct lost: %+v", v)
	}
}

// #4: a failing Close of the old file must not close the new one.
func TestCompactSurvivesOldCloseError(t *testing.T) {
	db, _ := openTemp(t)
	b := MustBucket[int, int](db, "c")
	must(t, b.Put(1, 1))
	db.f = &storageWrapper{storage: db.f, closeErr: errors.New("EIO on close")}
	must(t, db.Compact())
	must(t, b.Put(2, 2))
	if v, ok, err := b.Get(1); err != nil || !ok || v != 1 {
		t.Fatalf("after compact: %v %v %v", v, ok, err)
	}
}

// #5: keys that compare equal must encode identically.
func TestFloatKeyCanonicalization(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[float64, string](db, "f")
	must(t, b.Put(math.Copysign(0, -1), "zero"))
	if v, ok, _ := b.Get(0); !ok || v != "zero" {
		t.Fatal("-0.0 and +0.0 are different keys")
	}
	must(t, b.Put(math.NaN(), "nan"))
	must(t, b.Put(math.Inf(1), "inf"))
	if v, ok, _ := b.Get(math.Float64frombits(0x7FF8000000000ABC)); !ok || v != "nan" {
		t.Fatal("NaN payloads are different keys")
	}
	var keys []float64
	for k := range b.All().Keys() {
		keys = append(keys, k)
	}
	if len(keys) != 3 || keys[0] != 0 || !math.IsInf(keys[1], 1) || !math.IsNaN(keys[2]) {
		t.Fatalf("order %v", keys)
	}
	f32 := MustBucket[float32, int](db, "f32")
	must(t, f32.Put(float32(math.Copysign(0, -1)), 1))
	if _, ok, _ := f32.Get(0); !ok {
		t.Fatal("float32 -0")
	}
}

// #6: a commit that reported an fsync error must not survive, and the DB
// must stop accepting writes.
func TestFsyncFailurePoisonsWrites(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[string, string](db, "s")
	must(t, b.Put("kept", "v"))
	mem := db.f.(*memStorage)
	before := len(mem.b)
	db.opts.SyncWrites = true
	db.f = &storageWrapper{storage: mem, syncErr: errors.New("EIO")}

	if err := b.Put("lost", "v"); err == nil {
		t.Fatal("fsync error not reported")
	}
	if len(mem.b) != before {
		t.Fatalf("failed frame left in log: %d -> %d bytes", before, len(mem.b))
	}
	if _, ok, _ := b.Get("lost"); ok {
		t.Fatal("failed commit visible")
	}
	if err := b.Put("again", "v"); !errors.Is(err, ErrFailed) {
		t.Fatalf("write after fsync failure: %v", err)
	}
	if err := db.Update(func(*Tx) error { return nil }); !errors.Is(err, ErrFailed) {
		t.Fatalf("update after fsync failure: %v", err)
	}
	if v, ok, _ := b.Get("kept"); !ok || v != "v" {
		t.Fatal("reads stopped working")
	}
}

// #7: scans must bound memory by bytes, not only by entry count.
func TestScanChunkBoundedByBytes(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, []byte](db, "big")
	const n, size = 40, 256 << 10 // 10 MiB total, well under 128 entries
	for i := range n {
		must(t, b.Put(i, bytes.Repeat([]byte{byte(i)}, size)))
	}
	var ms0, ms1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms0)
	seen := 0
	must(t, db.scan(b.prefix, b.end, scanOpts{}, func(_, v []byte) bool {
		if len(v) != size || v[0] != byte(seen) {
			t.Fatalf("entry %d corrupt", seen)
		}
		seen++
		return true
	}))
	runtime.ReadMemStats(&ms1)
	if seen != n {
		t.Fatalf("saw %d of %d", seen, n)
	}
	if alloc := ms1.TotalAlloc - ms0.TotalAlloc; alloc > 6<<20 {
		t.Fatalf("scan allocated %d MiB for a %d MiB bucket", alloc>>20, n*size>>20)
	}
}

// #8: Clear and Drop are O(1) on disk and keep transaction ordering.
func TestClearIsRangeDelete(t *testing.T) {
	db, path := openTemp(t)
	b := MustBucket[int, int](db, "c")
	other := MustBucket[int, int](db, "other")
	must(t, other.Put(1, 1))
	must(t, db.Update(func(tx *Tx) error {
		bt := b.In(tx)
		for i := range 5000 {
			must(t, bt.Put(i, i))
		}
		return nil
	}))
	before := db.Stats().FileBytes
	must(t, b.Clear())
	if grew := db.Stats().FileBytes - before; grew > 100 {
		t.Fatalf("Clear wrote %d bytes", grew)
	}
	if n, _ := b.Count(); n != 0 {
		t.Fatalf("count after clear %d", n)
	}

	must(t, b.Put(1, 1))
	must(t, b.Put(2, 2))
	must(t, db.Update(func(tx *Tx) error {
		bt := b.In(tx)
		must(t, bt.Put(3, 3)) // before Clear: must be discarded
		must(t, bt.Clear())
		if _, ok, _ := bt.Get(1); ok {
			t.Error("committed key visible after Clear in tx")
		}
		must(t, bt.Put(4, 4)) // after Clear: must survive
		var keys []int
		it := bt.All()
		for k := range it.Keys() {
			keys = append(keys, k)
		}
		must(t, it.Err())
		if fmt.Sprint(keys) != "[4]" {
			t.Errorf("in-tx keys %v", keys)
		}
		return nil
	}))

	check := func(db *DB) {
		t.Helper()
		b := MustBucket[int, int](db, "c")
		var keys []int
		for k := range b.All().Keys() {
			keys = append(keys, k)
		}
		if fmt.Sprint(keys) != "[4]" {
			t.Fatalf("keys %v", keys)
		}
		if v, ok, _ := MustBucket[int, int](db, "other").Get(1); !ok || v != 1 {
			t.Fatal("Clear touched another bucket")
		}
	}
	check(db)
	must(t, db.Close())
	db2, err := Open(path, nil) // range deletes must replay
	must(t, err)
	defer db2.Close()
	check(db2)

	ns := db2.Namespace("t")
	nb := MustBucket[int, int](ns.Namespace("deep"), "b")
	must(t, nb.Put(1, 1))
	before = db2.Stats().FileBytes
	must(t, ns.Drop())
	if grew := db2.Stats().FileBytes - before; grew > 100 {
		t.Fatalf("Drop wrote %d bytes", grew)
	}
	if n, _ := nb.Count(); n != 0 {
		t.Fatal("Drop left nested data")
	}
	check(db2)
}

// #9: iterators report errors instead of panicking.
func TestIteratorErrorsDoNotPanic(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, int](db, "i")
	for i := range 300 {
		must(t, b.Put(i, i))
	}
	it := b.All()
	seen := 0
	for range it.Seq() {
		if seen++; seen == 1 {
			must(t, db.Close()) // e.g. graceful shutdown racing with a request
		}
	}
	if !errors.Is(it.Err(), ErrClosed) || seen != scanChunk {
		t.Fatalf("err=%v seen=%d", it.Err(), seen)
	}

	db2 := OpenMemory()
	b2 := MustBucket[int, int](db2, "i")
	var leaked *Bucket[int, int]
	must(t, db2.Update(func(tx *Tx) error { leaked = b2.In(tx); return nil }))
	it = leaked.All()
	for range it.Seq() {
	}
	if !errors.Is(it.Err(), ErrTxDone) {
		t.Fatalf("finished tx: %v", it.Err())
	}

	it = b2.Prefix(5, 1) // int keys have no prefix form
	for range it.Seq() {
		t.Fatal("yielded")
	}
	if it.Err() == nil {
		t.Fatal("no error for unsupported Prefix")
	}
}

// #10: Open must never clobber a file that is not a waffle database.
func TestOpenDoesNotClobberSmallForeignFile(t *testing.T) {
	dir := t.TempDir()
	foreign := filepath.Join(dir, "app.pid")
	must(t, os.WriteFile(foreign, []byte("4242\n"), 0o644))
	if _, err := Open(foreign, nil); err == nil {
		t.Fatal("opened a foreign file")
	}
	if data, _ := os.ReadFile(foreign); string(data) != "4242\n" {
		t.Fatalf("foreign file modified: %q", data)
	}

	partial := filepath.Join(dir, "partial.db") // crash while writing the header
	must(t, os.WriteFile(partial, []byte(magic[:3]), 0o644))
	db, err := Open(partial, nil)
	must(t, err)
	must(t, db.Close())
}

package waffle

import (
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock for TTL tests.
type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()) }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

func keysOf[K, V any](t *testing.T, it *Iter[K, V]) []K {
	t.Helper()
	var ks []K
	for k := range it.Keys() {
		ks = append(ks, k)
	}
	must(t, it.Err())
	return ks
}

// --- reverse iteration ---

func TestReverseIteration(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, string](db, "nums")
	for _, i := range []int{5, -3, 100, 0, -1000, 42} {
		must(t, b.Put(i, fmt.Sprint(i)))
	}
	if got := keysOf(t, b.All().Reverse()); !slices.Equal(got, []int{100, 42, 5, 0, -3, -1000}) {
		t.Fatalf("all reverse: %v", got)
	}
	if got := keysOf(t, b.Range(-3, 42).Reverse()); !slices.Equal(got, []int{5, 0, -3}) {
		t.Fatalf("range reverse: %v", got)
	}
	if got := keysOf(t, b.From(0).Reverse()); !slices.Equal(got, []int{100, 42, 5, 0}) {
		t.Fatalf("from reverse: %v", got)
	}
	var vals []string
	for k, v := range b.All().Reverse().Seq() {
		if v != fmt.Sprint(k) {
			t.Fatalf("value mismatch %d=%q", k, v)
		}
		vals = append(vals, v)
		if len(vals) == 2 {
			break
		}
	}
	if !slices.Equal(vals, []string{"100", "42"}) {
		t.Fatalf("early break: %v", vals)
	}
	var scanned []int
	must(t, b.Scan(Bounds[int]{Reverse: true}, func(k int, _ string) error {
		scanned = append(scanned, k)
		return nil
	}))
	if !slices.Equal(scanned, []int{100, 42, 5, 0, -3, -1000}) {
		t.Fatalf("scan reverse: %v", scanned)
	}
	// Other buckets must not leak into a reverse scan at either edge.
	must(t, MustBucket[int, string](db, "a").Put(1, "x"))
	must(t, MustBucket[int, string](db, "z").Put(1, "x"))
	if got := keysOf(t, b.All().Reverse()); len(got) != 6 {
		t.Fatalf("leak: %v", got)
	}
}

func TestReverseAcrossChunksWithConcurrentDeletes(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[uint32, uint32](db, "n")
	const n = scanChunk*3 + 17
	for i := range uint32(n) {
		must(t, b.Put(i, i))
	}
	// Delete keys ahead of the cursor while iterating. A scan is not a
	// snapshot, so a deleted key may still be yielded if its chunk was
	// already read; but order must hold, nothing may repeat, and every key
	// that was never deleted must be seen.
	deleted := map[uint32]bool{}
	var seen []uint32
	it := b.All().Reverse()
	for k, v := range it.Seq() {
		if k != v {
			t.Fatalf("value mismatch %d=%d", k, v)
		}
		if len(seen) > 0 && k >= seen[len(seen)-1] {
			t.Fatalf("not strictly descending: %d after %d", k, seen[len(seen)-1])
		}
		seen = append(seen, k)
		if k >= 2 && k%3 == 0 {
			must(t, b.Delete(k-2))
			deleted[k-2] = true
		}
	}
	must(t, it.Err())
	got := map[uint32]bool{}
	for _, k := range seen {
		got[k] = true
	}
	for i := range uint32(n) {
		if !deleted[i] && !got[i] {
			t.Fatalf("surviving key %d not yielded", i)
		}
	}
	if len(seen) == n {
		t.Fatal("no deletion ahead of the cursor was observed across chunks")
	}
}

func TestLatestEventWithFirst(t *testing.T) {
	type EventKey struct {
		OrderID string
		Seq     uint64
	}
	db := OpenMemory()
	events := MustBucket[EventKey, string](db, "events")
	for _, o := range []string{"A", "B", "C"} {
		for seq := range uint64(5) {
			must(t, events.Put(EventKey{o, seq}, fmt.Sprintf("%s-%d", o, seq)))
		}
	}
	k, v, ok, err := events.Prefix(EventKey{OrderID: "B"}, 1).Reverse().First()
	if err != nil || !ok || k.Seq != 4 || v != "B-4" {
		t.Fatalf("latest: %+v %q %v %v", k, v, ok, err)
	}
	k, _, ok, _ = events.Prefix(EventKey{OrderID: "B"}, 1).First()
	if !ok || k.Seq != 0 {
		t.Fatalf("earliest: %+v", k)
	}
	if _, _, ok, err := events.Prefix(EventKey{OrderID: "missing"}, 1).Reverse().First(); ok || err != nil {
		t.Fatalf("missing order: %v %v", ok, err)
	}
	if _, _, ok, err := events.Prefix(EventKey{}, 9).First(); ok || err == nil {
		t.Fatal("bad prefix must surface its error from First")
	}
}

func TestReverseTxMerge(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, int](db, "m")
	for _, k := range []int{1, 3, 5, 7} {
		must(t, b.Put(k, k))
	}
	must(t, db.Update(func(tx *Tx) error {
		bt := b.In(tx)
		must(t, bt.Put(0, 0))
		must(t, bt.Put(4, 4))
		must(t, bt.Put(5, 50))
		must(t, bt.Delete(3))
		must(t, bt.Put(9, 9))
		var got [][2]int
		it := bt.All().Reverse()
		for k, v := range it.Seq() {
			got = append(got, [2]int{k, v})
		}
		must(t, it.Err())
		want := [][2]int{{9, 9}, {7, 7}, {5, 50}, {4, 4}, {1, 1}, {0, 0}}
		if !slices.Equal(got, want) {
			t.Errorf("reverse merged scan %v", got)
		}
		must(t, bt.Clear())
		must(t, bt.Put(2, 2))
		if got := keysOf(t, bt.All().Reverse()); !slices.Equal(got, []int{2}) {
			t.Errorf("after clear: %v", got)
		}
		return nil
	}))
}

// The skiplist's backward links must stay consistent with its forward
// links through any mix of inserts, overwrites, deletes and range deletes.
func TestIndexBackwardLinks(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 9))
	ix := newIndex()
	key := func(i int) []byte { return []byte(fmt.Sprintf("k%04d", i)) }
	for step := range 20000 {
		switch op := r.IntN(10); {
		case op < 6:
			ix.set(key(r.IntN(2000)), int64(step), 1, 0)
		case op < 9:
			ix.delete(key(r.IntN(2000)))
		default:
			lo := r.IntN(2000)
			ix.deleteRange(key(lo), key(lo+r.IntN(20)))
		}
		if step%997 != 0 {
			continue
		}
		var fwd, bwd [][]byte
		for n := ix.first(); n != nil; n = n.next[0] {
			fwd = append(fwd, n.key)
		}
		for n := ix.seekLT(nil); n != nil; n = n.prev {
			bwd = append(bwd, n.key)
		}
		slices.Reverse(bwd)
		if !slices.EqualFunc(fwd, bwd, func(a, b []byte) bool { return string(a) == string(b) }) || len(fwd) != ix.n {
			t.Fatalf("step %d: forward %d keys, backward %d, n=%d", step, len(fwd), len(bwd), ix.n)
		}
	}
}

// --- TTL ---

func openClocked(t *testing.T, path string, c *fakeClock) *DB {
	t.Helper()
	db, err := Open(path, &Options{Now: c.Now})
	must(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestTTLExpiry(t *testing.T) {
	clk := newFakeClock()
	db := openClocked(t, MemoryPath, clk)
	b := MustBucket[string, string](db, "sessions")
	must(t, b.PutTTL("short", "a", time.Minute))
	must(t, b.PutTTL("long", "b", time.Hour))
	must(t, b.Put("forever", "c"))

	at, ok, err := b.ExpiresAt("short")
	if err != nil || !ok || !at.Equal(clk.Now().Add(time.Minute)) {
		t.Fatalf("ExpiresAt: %v %v %v", at, ok, err)
	}
	if at, ok, _ := b.ExpiresAt("forever"); !ok || !at.IsZero() {
		t.Fatalf("no-TTL ExpiresAt: %v %v", at, ok)
	}

	clk.Advance(time.Minute) // exactly at expiry: gone
	if _, ok, _ := b.Get("short"); ok {
		t.Fatal("expired entry visible via Get")
	}
	if ok, _ := b.Has("short"); ok {
		t.Fatal("expired entry visible via Has")
	}
	if _, ok, _ := b.ExpiresAt("short"); ok {
		t.Fatal("expired entry visible via ExpiresAt")
	}
	if n, _ := b.Count(); n != 2 {
		t.Fatalf("count %d", n)
	}
	if got := keysOf(t, b.All()); !slices.Equal(got, []string{"forever", "long"}) {
		t.Fatalf("forward keys %v", got)
	}
	if got := keysOf(t, b.All().Reverse()); !slices.Equal(got, []string{"long", "forever"}) {
		t.Fatalf("reverse keys %v", got)
	}

	// Plain Put on a bucket without a default TTL clears the TTL.
	must(t, b.Put("long", "b2"))
	clk.Advance(2 * time.Hour)
	if v, ok, _ := b.Get("long"); !ok || v != "b2" {
		t.Fatal("Put did not clear the TTL")
	}

	if err := b.PutTTL("x", "y", 0); err == nil {
		t.Fatal("zero TTL accepted")
	}
	must(t, b.PutTTL("huge", "y", time.Duration(math.MaxInt64))) // must not overflow into the past
	if _, ok, _ := b.Get("huge"); !ok {
		t.Fatal("huge TTL overflowed")
	}
}

func TestDefaultTTLAndUpdate(t *testing.T) {
	clk := newFakeClock()
	db := openClocked(t, MemoryPath, clk)
	if _, err := NewBucket[string, int](db, "bad", WithTTL(-time.Second)); err == nil {
		t.Fatal("negative default TTL accepted")
	}
	// A fixed-window rate limiter: the window starts at the first hit and
	// increments must not extend it.
	hits := MustBucket[string, int](db, "ratelimit", WithTTL(time.Minute))
	incr := func() {
		must(t, hits.Update("user-1", func(n int, _ bool) (int, error) { return n + 1, nil }))
	}
	incr()
	start := clk.Now()
	clk.Advance(30 * time.Second)
	incr()
	incr()
	if n, _, _ := hits.Get("user-1"); n != 3 {
		t.Fatalf("count %d", n)
	}
	if at, _, _ := hits.ExpiresAt("user-1"); !at.Equal(start.Add(time.Minute)) {
		t.Fatalf("Update moved the expiry to %v", at)
	}
	clk.Advance(30 * time.Second)
	if _, ok, _ := hits.Get("user-1"); ok {
		t.Fatal("window did not expire")
	}
	incr() // new window
	if n, _, _ := hits.Get("user-1"); n != 1 {
		t.Fatalf("new window count %d", n)
	}

	must(t, hits.PutTTL("custom", 1, time.Hour)) // per-entry override
	if at, _, _ := hits.ExpiresAt("custom"); !at.Equal(clk.Now().Add(time.Hour)) {
		t.Fatalf("PutTTL override: %v", at)
	}
}

func TestExpire(t *testing.T) {
	clk := newFakeClock()
	db := openClocked(t, MemoryPath, clk)
	b := MustBucket[string, string](db, "s")
	must(t, b.PutTTL("sess", "data", time.Minute))
	clk.Advance(50 * time.Second)
	ok, err := b.Expire("sess", time.Minute) // sliding expiration on activity
	if err != nil || !ok {
		t.Fatalf("Expire: %v %v", ok, err)
	}
	clk.Advance(50 * time.Second)
	if v, ok, _ := b.Get("sess"); !ok || v != "data" {
		t.Fatal("Expire did not extend the session")
	}
	if ok, _ := b.Expire("missing", time.Minute); ok {
		t.Fatal("Expire reported a missing key as found")
	}
	if _, err := b.Expire("sess", -1); err == nil {
		t.Fatal("negative TTL accepted")
	}
	must(t, b.Put("plain", "v"))
	if ok, _ := b.Expire("plain", time.Second); !ok {
		t.Fatal("Expire on key without TTL")
	}
	clk.Advance(time.Second)
	if _, ok, _ := b.Get("plain"); ok {
		t.Fatal("Expire did not add a TTL")
	}
}

func TestTTLInTransaction(t *testing.T) {
	clk := newFakeClock()
	db := openClocked(t, MemoryPath, clk)
	b := MustBucket[string, int](db, "t")
	must(t, db.Update(func(tx *Tx) error {
		bt := b.In(tx)
		must(t, bt.PutTTL("a", 1, time.Minute))
		must(t, bt.Put("b", 2))
		if at, ok, _ := bt.ExpiresAt("a"); !ok || at.IsZero() {
			t.Error("pending TTL not visible in tx")
		}
		clk.Advance(time.Minute)
		if _, ok, _ := bt.Get("a"); ok {
			t.Error("expired pending write visible in tx")
		}
		if got := keysOf(t, bt.All()); !slices.Equal(got, []string{"b"}) {
			t.Errorf("tx scan %v", got)
		}
		return nil
	}))
	if _, ok, _ := b.Get("a"); ok {
		t.Fatal("expired write visible after commit")
	}
}

func TestTTLPersistenceCompactAndPurge(t *testing.T) {
	clk := newFakeClock()
	path := filepath.Join(t.TempDir(), "ttl.db")
	db, err := Open(path, &Options{Now: clk.Now})
	must(t, err)
	b := MustBucket[string, string](db, "s")
	must(t, b.PutTTL("hour", "h", time.Hour))
	must(t, b.Put("shadowed", "old-forever"))         // an older, non-expiring version...
	must(t, b.PutTTL("shadowed", "new", time.Minute)) // ...overwritten by an expiring one
	must(t, b.Put("forever", "f"))
	for i := range 200 {
		must(t, b.PutTTL(fmt.Sprint("tmp", i), "x", time.Minute))
	}
	hourAt, _, _ := b.ExpiresAt("hour")
	must(t, db.Close())

	clk.Advance(2 * time.Minute)
	check := func(db *DB) {
		t.Helper()
		b := MustBucket[string, string](db, "s")
		if got := keysOf(t, b.All()); !slices.Equal(got, []string{"forever", "hour"}) {
			t.Fatalf("keys %v", got)
		}
		if _, ok, _ := b.Get("shadowed"); ok {
			t.Fatal("expired write resurrected the older value")
		}
		if at, _, _ := b.ExpiresAt("hour"); !at.Equal(hourAt) {
			t.Fatalf("TTL not preserved: %v vs %v", at, hourAt)
		}
	}
	db, err = Open(path, &Options{Now: clk.Now})
	must(t, err)
	check(db)
	if k := db.Stats().Keys; k != 2 {
		t.Fatalf("replay kept %d keys; expired puts should replay as deletes", k)
	}

	must(t, MustBucket[string, string](db, "s").PutTTL("soon", "x", time.Second))
	clk.Advance(time.Second)
	if n, err := db.PurgeExpired(); err != nil || n != 1 {
		t.Fatalf("purge: %d %v", n, err)
	}
	if k := db.Stats().Keys; k != 2 {
		t.Fatalf("keys after purge %d", k)
	}

	before := db.Stats().FileBytes
	must(t, db.Compact())
	if after := db.Stats().FileBytes; after >= before/2 {
		t.Fatalf("compact did not drop expired data: %d -> %d", before, after)
	}
	check(db)
	must(t, db.Close())
	db, err = Open(path, &Options{Now: clk.Now})
	must(t, err)
	defer db.Close()
	check(db)

	clk.Advance(time.Hour) // the "hour" entry now expires
	if _, ok, _ := MustBucket[string, string](db, "s").Get("hour"); ok {
		t.Fatal("TTL lost through compaction")
	}
}

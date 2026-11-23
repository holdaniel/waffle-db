package waffle

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

type User struct {
	Name  string
	Email string
	Age   int
}

func openTemp(t *testing.T) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}

func TestBucketCRUD(t *testing.T) {
	db := OpenMemory()
	users := MustBucket[uint64, User](db, "users")

	if _, ok, err := users.Get(1); ok || err != nil {
		t.Fatalf("empty get: %v %v", ok, err)
	}
	must(t, users.Put(1, User{Name: "Ada", Age: 36}))
	must(t, users.Put(2, User{Name: "Linus"}))
	u, ok, err := users.Get(1)
	if !ok || err != nil || u.Name != "Ada" || u.Age != 36 {
		t.Fatalf("get: %+v %v %v", u, ok, err)
	}
	must(t, users.Put(1, User{Name: "Ada L."}))
	u, _, _ = users.Get(1)
	if u.Name != "Ada L." {
		t.Fatalf("overwrite: %+v", u)
	}
	must(t, users.Delete(1))
	must(t, users.Delete(99)) // missing: no error
	if ok, _ := users.Has(1); ok {
		t.Fatal("deleted key still present")
	}
	if n, _ := users.Count(); n != 1 {
		t.Fatalf("count = %d", n)
	}
}

func TestOrderedIteration(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, string](db, "nums")
	for _, i := range []int{5, -3, 100, 0, -1000, 42} {
		must(t, b.Put(i, fmt.Sprint(i)))
	}
	var keys []int
	for k, v := range b.All().Seq() {
		if v != fmt.Sprint(k) {
			t.Fatalf("value mismatch %d %q", k, v)
		}
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []int{-1000, -3, 0, 5, 42, 100}) {
		t.Fatalf("order: %v", keys)
	}
	keys = keys[:0]
	for k := range b.Range(-3, 42).Seq() {
		keys = append(keys, k)
	}
	if !slices.Equal(keys, []int{-3, 0, 5}) {
		t.Fatalf("range: %v", keys)
	}
	// early break
	for k := range b.From(0).Seq() {
		if k != 0 {
			t.Fatal("from")
		}
		break
	}
}

func TestIterationLargerThanChunk(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[uint32, uint32](db, "n")
	const n = scanChunk*3 + 17
	for i := range uint32(n) {
		must(t, b.Put(i, i*2))
	}
	var i uint32
	for k, v := range b.All().Seq() {
		if k != i || v != i*2 {
			t.Fatalf("at %d got %d=%d", i, k, v)
		}
		i++
	}
	if i != n {
		t.Fatalf("saw %d of %d", i, n)
	}
}

func TestWriteDuringIteration(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, int](db, "n")
	for i := range 300 {
		must(t, b.Put(i, i))
	}
	for k, v := range b.All().Seq() { // must not deadlock
		must(t, b.Put(k, v+1))
	}
	v, _, _ := b.Get(299)
	if v != 300 {
		t.Fatalf("got %d", v)
	}
}

func TestCompositeKeyPrefix(t *testing.T) {
	type OrderKey struct {
		UserID uint64
		At     time.Time
	}
	db := OpenMemory()
	orders := MustBucket[OrderKey, float64](db, "orders")
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for u := uint64(1); u <= 3; u++ {
		for d := range 4 {
			must(t, orders.Put(OrderKey{u, t0.Add(time.Duration(3-d) * time.Hour)}, float64(u)))
		}
	}
	it := orders.Prefix(OrderKey{UserID: 2}, 1)
	var got []time.Time
	for k, v := range it.Seq() {
		if k.UserID != 2 || v != 2 {
			t.Fatalf("leaked %+v", k)
		}
		got = append(got, k.At)
	}
	must(t, it.Err())
	if len(got) != 4 || !slices.IsSortedFunc(got, time.Time.Compare) {
		t.Fatalf("prefix scan: %v", got)
	}
}

func TestStringPrefix(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[string, int](db, "s")
	for i, k := range []string{"app", "apple", "apply", "apricot", "banana", "ap"} {
		must(t, b.Put(k, i))
	}
	it := b.Prefix("app", 0)
	var keys []string
	for k := range it.Keys() {
		keys = append(keys, k)
	}
	must(t, it.Err())
	if !slices.Equal(keys, []string{"app", "apple", "apply"}) {
		t.Fatalf("got %v", keys)
	}
}

func TestNamespacesIsolate(t *testing.T) {
	db := OpenMemory()
	a := MustBucket[string, string](db.Namespace("tenant-a"), "kv")
	b := MustBucket[string, string](db.Namespace("tenant-b"), "kv")
	top := MustBucket[string, string](db, "kv")
	nested := MustBucket[string, string](db.Namespace("tenant-a").Namespace("kv"), "kv")
	must(t, a.Put("k", "a"))
	must(t, b.Put("k", "b"))
	must(t, top.Put("k", "top"))
	must(t, nested.Put("k", "nested"))

	for want, bk := range map[string]*Bucket[string, string]{"a": a, "b": b, "top": top, "nested": nested} {
		if n, _ := bk.Count(); n != 1 {
			t.Fatalf("%s count=%d", want, n)
		}
		if v, _, _ := bk.Get("k"); v != want {
			t.Fatalf("got %q want %q", v, want)
		}
	}
	must(t, db.Namespace("tenant-a").Drop())
	if n, _ := a.Count(); n != 0 {
		t.Fatal("drop left data")
	}
	if n, _ := nested.Count(); n != 0 {
		t.Fatal("drop left nested data")
	}
	if v, _, _ := b.Get("k"); v != "b" {
		t.Fatal("drop touched sibling")
	}
}

func TestTransactions(t *testing.T) {
	db := OpenMemory()
	acct := MustBucket[string, int](db, "accounts")
	must(t, acct.Put("alice", 100))
	must(t, acct.Put("bob", 0))

	transfer := func(amount int) error {
		return db.Update(func(tx *Tx) error {
			a := acct.In(tx)
			bal, _, _ := a.Get("alice")
			if bal < amount {
				return errors.New("insufficient funds")
			}
			must(t, a.Put("alice", bal-amount))
			must(t, a.Update("bob", func(v int, _ bool) (int, error) { return v + amount, nil }))
			// read-your-writes, including in scans
			sum := 0
			for _, v := range a.All().Seq() {
				sum += v
			}
			if sum != 100 {
				t.Errorf("in-tx sum = %d", sum)
			}
			return nil
		})
	}
	must(t, transfer(30))
	if err := transfer(500); err == nil {
		t.Fatal("expected failure")
	}
	al, _, _ := acct.Get("alice")
	bo, _, _ := acct.Get("bob")
	if al != 70 || bo != 30 {
		t.Fatalf("alice=%d bob=%d", al, bo)
	}
}

func TestTxScanMerge(t *testing.T) {
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
		for k, v := range bt.All().Seq() {
			got = append(got, [2]int{k, v})
		}
		want := [][2]int{{0, 0}, {1, 1}, {4, 4}, {5, 50}, {7, 7}, {9, 9}}
		if !slices.Equal(got, want) {
			t.Fatalf("merged scan %v", got)
		}
		return nil
	}))
}

func TestTxPanicRollsBack(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, int](db, "p")
	func() {
		defer func() { recover() }()
		_ = db.Update(func(tx *Tx) error {
			must(t, b.In(tx).Put(1, 1))
			panic("boom")
		})
	}()
	if ok, _ := b.Has(1); ok {
		t.Fatal("panicked tx committed")
	}
	must(t, b.Put(2, 2)) // writer lock was released
}

func TestPersistenceAndRecovery(t *testing.T) {
	db, path := openTemp(t)
	b := MustBucket[string, User](db, "users")
	for i := range 50 {
		must(t, b.Put(fmt.Sprintf("u%02d", i), User{Name: fmt.Sprint(i), Age: i}))
	}
	must(t, b.Delete("u07"))
	must(t, db.Close())
	good, _ := os.Stat(path)

	// Simulate a crash mid-write: append a torn frame.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0xff, 0, 0, 0, 1, 2, 3})
	f.Close()

	db2, err := Open(path, nil)
	must(t, err)
	defer db2.Close()
	if st := db2.Stats(); st.TruncatedBytes != 7 || st.FileBytes != good.Size() {
		t.Fatalf("recovery stats %+v (good size %d)", st, good.Size())
	}
	b2 := MustBucket[string, User](db2, "users")
	if n, _ := b2.Count(); n != 49 {
		t.Fatalf("count after reopen = %d", n)
	}
	u, ok, _ := b2.Get("u42")
	if !ok || u.Age != 42 {
		t.Fatalf("u42 = %+v", u)
	}
	must(t, b2.Put("after", User{Name: "x"})) // appends after the truncated tail
}

func TestCorruptFrameTruncates(t *testing.T) {
	db, path := openTemp(t)
	b := MustBucket[int, int](db, "x")
	must(t, b.Put(1, 1))
	must(t, b.Put(2, 2))
	must(t, db.Close())
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff // flip a byte in the last frame
	os.WriteFile(path, data, 0o644)

	db2, err := Open(path, nil)
	must(t, err)
	defer db2.Close()
	b2 := MustBucket[int, int](db2, "x")
	if ok, _ := b2.Has(1); !ok {
		t.Fatal("lost intact frame")
	}
	if ok, _ := b2.Has(2); ok {
		t.Fatal("corrupt frame applied")
	}
}

func TestCompact(t *testing.T) {
	for _, mem := range []bool{false, true} {
		t.Run(fmt.Sprint("mem=", mem), func(t *testing.T) {
			var db *DB
			var path string
			if mem {
				db = OpenMemory()
			} else {
				db, path = openTemp(t)
			}
			b := MustBucket[int, string](db, "c")
			for round := range 10 {
				for i := range 100 {
					must(t, b.Put(i, fmt.Sprintf("v%d-%d", i, round)))
				}
			}
			for i := 50; i < 100; i++ {
				must(t, b.Delete(i))
			}
			before := db.Stats()
			must(t, db.Compact())
			after := db.Stats()
			if after.FileBytes >= before.FileBytes/5 {
				t.Fatalf("compaction ineffective: %d -> %d", before.FileBytes, after.FileBytes)
			}
			check := func(b *Bucket[int, string]) {
				if n, _ := b.Count(); n != 50 {
					t.Fatalf("count %d", n)
				}
				if v, _, _ := b.Get(7); v != "v7-9" {
					t.Fatalf("got %q", v)
				}
			}
			check(b)
			must(t, b.Put(1000, "new"))
			if !mem {
				must(t, db.Close())
				db2, err := Open(path, nil)
				must(t, err)
				defer db2.Close()
				b2 := MustBucket[int, string](db2, "c")
				must(t, b2.Delete(1000))
				check(b2)
			}
		})
	}
}

func TestConcurrentReadersWriters(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, int](db, "c")
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 500 {
				must(t, b.Put(w*1000+i, i))
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			for range 50 {
				for range b.All().Seq() {
				}
				_, _, _ = b.Get(3)
			}
		})
	}
	wg.Go(func() {
		for range 5 {
			must(t, db.Compact())
		}
	})
	wg.Wait()
	if n, _ := b.Count(); n != 2000 {
		t.Fatalf("count %d", n)
	}
}

func TestCustomCodecAndErrors(t *testing.T) {
	db := OpenMemory()
	j := MustBucket[string, User](db, "json", WithCodec(JSON[User]()))
	must(t, j.Put("a", User{Name: "A"}))
	if u, _, _ := j.Get("a"); u.Name != "A" {
		t.Fatal("json codec")
	}
	if _, err := NewBucket[string, User](db, "bad", WithCodec(JSON[int]())); err == nil {
		t.Fatal("mismatched codec accepted")
	}
	if _, err := NewBucket[string, chan int](db, "bad"); err == nil {
		t.Fatal("chan value accepted")
	}
	must(t, db.Close())
	if err := j.Put("b", User{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("put after close: %v", err)
	}
	err := j.Scan(Bounds[string]{}, func(string, User) error { return nil })
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("scan after close: %v", err)
	}
}

func TestScanStop(t *testing.T) {
	db := OpenMemory()
	b := MustBucket[int, int](db, "s")
	for i := range 10 {
		must(t, b.Put(i, i))
	}
	from, to := 2, 8
	var seen []int
	err := b.Scan(Bounds[int]{From: &from, To: &to}, func(k, _ int) error {
		seen = append(seen, k)
		if k == 4 {
			return ErrStop
		}
		return nil
	})
	if err != nil || !slices.Equal(seen, []int{2, 3, 4}) {
		t.Fatalf("%v %v", seen, err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

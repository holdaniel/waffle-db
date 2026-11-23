package waffle

import (
	"path/filepath"
	"testing"
	"time"
)

type benchUser struct {
	Name  string
	Email string
	Age   int
	Tags  []string
	Score float64
}

var sampleUser = benchUser{Name: "Ada Lovelace", Email: "ada@example.com", Age: 36, Tags: []string{"math", "engines"}, Score: 99.5}

func BenchmarkCodecCompiledAppend(b *testing.B) {
	c, _ := Compiled[benchUser]()
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf, _ = c.Append(buf[:0], sampleUser)
	}
}

func BenchmarkCodecJSONAppend(b *testing.B) {
	c := JSON[benchUser]()
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf, _ = c.Append(buf[:0], sampleUser)
	}
}

func BenchmarkCodecCompiledDecode(b *testing.B) {
	c, _ := Compiled[benchUser]()
	enc, _ := c.Append(nil, sampleUser)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.Decode(enc)
	}
}

func BenchmarkCodecJSONDecode(b *testing.B) {
	c := JSON[benchUser]()
	enc, _ := c.Append(nil, sampleUser)
	b.ReportAllocs()
	for b.Loop() {
		_, _ = c.Decode(enc)
	}
}

func BenchmarkKeyEncodeStruct(b *testing.B) {
	type K struct {
		Tenant string
		ID     uint64
	}
	kc, _ := OrderedKey[K]()
	buf := make([]byte, 0, 64)
	k := K{"tenant-42", 1234567}
	b.ReportAllocs()
	for b.Loop() {
		buf = kc.AppendKey(buf[:0], k)
	}
}

func BenchmarkPutUint64(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "b.db"), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	bk := MustBucket[uint64, uint64](db, "n")
	b.ReportAllocs()
	i := uint64(0)
	for b.Loop() {
		_ = bk.Put(i%10000, i) // overwrite a bounded key set: steady state
		i++
	}
}

func BenchmarkGetUint64(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "b.db"), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	bk := MustBucket[uint64, uint64](db, "n")
	for i := range uint64(10000) {
		_ = bk.Put(i, i)
	}
	b.ReportAllocs()
	i := uint64(0)
	for b.Loop() {
		_, _, _ = bk.Get(i % 10000)
		i++
	}
}

func BenchmarkGetStruct(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "b.db"), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	bk := MustBucket[uint64, benchUser](db, "u")
	for i := range uint64(10000) {
		_ = bk.Put(i, sampleUser)
	}
	b.ReportAllocs()
	i := uint64(0)
	for b.Loop() {
		_, _, _ = bk.Get(i % 10000)
		i++
	}
}

func BenchmarkIterate10k(b *testing.B) {
	db := OpenMemory()
	bk := MustBucket[uint64, uint64](db, "n")
	for i := range uint64(10000) {
		_ = bk.Put(i, i)
	}
	b.ReportAllocs()
	for b.Loop() {
		for range bk.All().Seq() {
		}
	}
}

func BenchmarkIterate10kReverse(b *testing.B) {
	db := OpenMemory()
	bk := MustBucket[uint64, uint64](db, "n")
	for i := range uint64(10000) {
		_ = bk.Put(i, i)
	}
	b.ReportAllocs()
	for b.Loop() {
		for range bk.All().Reverse().Seq() {
		}
	}
}

func BenchmarkPutTTL(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "b.db"), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	bk := MustBucket[uint64, uint64](db, "n", WithTTL(time.Hour))
	b.ReportAllocs()
	i := uint64(0)
	for b.Loop() {
		_ = bk.Put(i%10000, i)
		i++
	}
}

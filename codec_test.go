package waffle

import (
	"bytes"
	"cmp"
	"math"
	"math/rand/v2"
	"net/netip"
	"reflect"
	"slices"
	"testing"
	"time"
)

// checkOrder verifies that the key encoding preserves the ordering given by
// cmpFn and round-trips every value.
func checkOrder[K any](t *testing.T, vals []K, cmpFn func(a, b K) int) {
	t.Helper()
	kc, err := OrderedKey[K]()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range vals {
		ea := kc.AppendKey(nil, a)
		got, err := kc.DecodeKey(ea)
		if err != nil || cmpFn(got, a) != 0 {
			t.Fatalf("round trip %v: got %v err %v", a, got, err)
		}
		for _, b := range vals {
			eb := kc.AppendKey(nil, b)
			if want, have := cmpFn(a, b), bytes.Compare(ea, eb); sign(want) != sign(have) {
				t.Fatalf("order mismatch %v vs %v: logical %d, bytes %d (%x vs %x)", a, b, want, have, ea, eb)
			}
		}
	}
}

func sign(x int) int { return cmp.Compare(x, 0) }

func TestKeyOrderInts(t *testing.T) {
	ints := []int64{math.MinInt64, -1 << 40, -256, -1, 0, 1, 255, 256, 1 << 40, math.MaxInt64}
	checkOrder(t, ints, cmp.Compare[int64])
	i8 := []int8{-128, -1, 0, 1, 127}
	checkOrder(t, i8, cmp.Compare[int8])
	u := []uint32{0, 1, 255, 256, math.MaxUint32}
	checkOrder(t, u, cmp.Compare[uint32])
	type UserID uint64 // named types work too
	checkOrder(t, []UserID{0, 7, 1 << 63}, cmp.Compare[UserID])
}

func TestKeyOrderFloats(t *testing.T) {
	fs := []float64{math.Inf(-1), -1e300, -2.5, -1, -math.SmallestNonzeroFloat64, 0, math.SmallestNonzeroFloat64, 1, 2.5, 1e300, math.Inf(1)}
	checkOrder(t, fs, cmp.Compare[float64])
	f32 := []float32{float32(math.Inf(-1)), -3, -0.5, 0, 0.5, 3, float32(math.Inf(1))}
	checkOrder(t, f32, cmp.Compare[float32])
}

func TestKeyOrderStrings(t *testing.T) {
	ss := []string{"", "\x00", "\x00\x00", "\x00\x01", "\x00\xff", "a", "a\x00", "a\x00b", "a\x01", "ab", "abc", "b", "\xff", "\xff\xff"}
	checkOrder(t, ss, cmp.Compare[string])
	checkOrder(t, [][]byte{nil, {0}, {0, 0}, {1}, {0xff}}, bytes.Compare)
}

func TestKeyOrderComposite(t *testing.T) {
	type K struct {
		Tenant string
		At     time.Time
		Seq    int32
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var ks []K
	for _, tn := range []string{"", "a", "a\x00", "ab"} {
		for _, d := range []time.Duration{-time.Hour, 0, time.Nanosecond, time.Hour} {
			for _, s := range []int32{-5, 0, 5} {
				ks = append(ks, K{tn, base.Add(d), s})
			}
		}
	}
	checkOrder(t, ks, func(a, b K) int {
		return cmp.Or(cmp.Compare(a.Tenant, b.Tenant), a.At.Compare(b.At), cmp.Compare(a.Seq, b.Seq))
	})
}

func TestKeyOrderRandomized(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	var vals []int64
	var strs []string
	for range 300 {
		vals = append(vals, int64(r.Uint64()))
		b := make([]byte, r.IntN(6))
		for i := range b {
			b[i] = byte(r.IntN(4)) // lots of 0x00 to stress escaping
		}
		strs = append(strs, string(b))
	}
	checkOrder(t, vals, cmp.Compare[int64])
	checkOrder(t, strs, cmp.Compare[string])
}

func TestUnsupportedKey(t *testing.T) {
	if _, err := OrderedKey[map[string]int](); err == nil {
		t.Fatal("expected error for map key")
	}
	if _, err := OrderedKey[*int](); err == nil {
		t.Fatal("expected error for pointer key")
	}
}

type Address struct {
	City string
	Zip  [5]byte
}

type Profile struct {
	Name     string
	Age      uint8
	Score    float64
	Balance  int64
	Active   bool
	Tags     []string
	Raw      []byte
	Home     *Address
	Prev     []Address
	Meta     map[string]int
	Created  time.Time
	IP       netip.Addr
	Z        complex128
	Internal string `waffle:"-"`
	private  int
}

func TestCompiledRoundTrip(t *testing.T) {
	c, err := Compiled[Profile]()
	if err != nil {
		t.Fatal(err)
	}
	in := Profile{
		Name: "Ada", Age: 36, Score: -1.5, Balance: -42, Active: true,
		Tags: []string{"x", "", "z"}, Raw: []byte{0, 1, 2},
		Home:     &Address{City: "London", Zip: [5]byte{'N', '1'}},
		Prev:     []Address{{City: "Paris"}},
		Meta:     map[string]int{"a": 1, "b": -2},
		Created:  time.Date(2026, 11, 23, 10, 0, 0, 7, time.UTC),
		IP:       netip.MustParseAddr("10.0.0.1"),
		Z:        complex(1, -2),
		Internal: "skip", private: 1,
	}
	enc, err := c.Append(nil, in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.Decode(enc)
	if err != nil {
		t.Fatal(err)
	}
	in.Internal, in.private = "", 0
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch\n in: %+v\nout: %+v", in, out)
	}

	// nil vs empty is preserved.
	enc, _ = c.Append(nil, Profile{Tags: []string{}})
	out, _ = c.Decode(enc)
	if out.Tags == nil || out.Raw != nil || out.Meta != nil || out.Home != nil {
		t.Fatalf("nil/empty not preserved: %+v", out)
	}
}

func TestCompiledRecursive(t *testing.T) {
	type Node struct {
		Val  int
		Next *Node
		Kids []Node
	}
	c, err := Compiled[Node]()
	if err != nil {
		t.Fatal(err)
	}
	in := Node{1, &Node{2, &Node{Val: 3}, nil}, []Node{{Val: 4}}}
	enc, _ := c.Append(nil, in)
	out, err := c.Decode(enc)
	if err != nil || !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v err %v", out, err)
	}
}

func TestSchemaEvolution(t *testing.T) {
	type V1 struct{ A, B string }
	type V2 struct {
		A, B string
		C    int
	}
	c1, _ := Compiled[V1]()
	c2, _ := Compiled[V2]()
	old, _ := c1.Append(nil, V1{"a", "b"})
	v2, err := c2.Decode(old) // appended field: old data still decodes
	if err != nil || v2 != (V2{"a", "b", 0}) {
		t.Fatalf("old->new: %+v %v", v2, err)
	}
	nw, _ := c2.Append(nil, V2{"a", "b", 3})
	if _, err := c1.Decode(nw); err == nil {
		t.Fatal("new->old should fail loudly, not silently drop data")
	}
}

func TestCompiledRejectsCorruptInput(t *testing.T) {
	c, _ := Compiled[Profile]()
	enc, _ := c.Append(nil, Profile{Name: "x", Tags: []string{"a", "b"}})
	for i := range enc {
		_, _ = c.Decode(enc[:i]) // must not panic
	}
	huge := []byte{1, 0xff, 0xff, 0xff, 0xff, 0x0f} // 1 field, string len ~4G
	if _, err := c.Decode(huge); err == nil {
		t.Fatal("expected error")
	}
}

func TestUnsupportedValue(t *testing.T) {
	type Bad struct{ F func() }
	if _, err := Compiled[Bad](); err == nil {
		t.Fatal("expected error for func field")
	}
	if _, err := Compiled[any](); err == nil {
		t.Fatal("expected error for interface")
	}
}

func FuzzStringKey(f *testing.F) {
	f.Add("a", "b")
	f.Add("a\x00", "a")
	kc, _ := OrderedKey[string]()
	f.Fuzz(func(t *testing.T, a, b string) {
		ea, eb := kc.AppendKey(nil, a), kc.AppendKey(nil, b)
		if sign(bytes.Compare(ea, eb)) != sign(cmp.Compare(a, b)) {
			t.Fatalf("order %q %q", a, b)
		}
		if got, err := kc.DecodeKey(ea); err != nil || got != a {
			t.Fatalf("round trip %q: %q %v", a, got, err)
		}
	})
}

func FuzzDecodeNoPanic(f *testing.F) {
	c, _ := Compiled[Profile]()
	seed, _ := c.Append(nil, Profile{Name: "seed", Tags: []string{"t"}, Meta: map[string]int{"k": 1}})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = c.Decode(b)
	})
}

var _ = slices.Sort[[]int]

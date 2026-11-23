package waffle

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"sync"
	"unsafe"
)

// store is what a Bucket reads and writes through: the DB itself
// (auto-commit) or a Tx.
type store interface {
	get(key, dst []byte) ([]byte, bool, error)
	put(key, val []byte) error
	del(key []byte) error
	scan(lo, hi []byte, keysOnly bool, fn func(k, v []byte) bool) error
}

// Parent is a container for buckets and namespaces: a *DB or a *Namespace.
type Parent interface {
	parentPrefix() []byte
	parentDB() *DB
}

func (db *DB) parentPrefix() []byte { return nil }
func (db *DB) parentDB() *DB        { return db }

// Key-space layout: every namespace or bucket name becomes a tagged,
// escaped, terminated segment, so siblings can never overlap and a bucket
// can never collide with a namespace of the same name:
//
//	ns "app" / ns "tenant-1" / bucket "users" / <ordered key>
//	01 app 00 01  01 tenant-1 00 01  02 users 00 01  ...
const (
	tagNamespace = 0x01
	tagBucket    = 0x02
)

func childPrefix(parent []byte, tag byte, name string) []byte {
	p := make([]byte, 0, len(parent)+len(name)+3)
	p = append(append(p, parent...), tag)
	return appendTerminated(p, name)
}

// prefixEnd returns the smallest key greater than every key starting with p,
// or nil if there is none.
func prefixEnd(p []byte) []byte {
	end := bytes.Clone(p)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// Namespace groups buckets (and further namespaces) under a common key
// prefix, e.g. per tenant. Namespaces are free: nothing is stored until a
// bucket inside one is written.
type Namespace struct {
	db     *DB
	prefix []byte
}

// Namespace returns the top-level namespace called name.
func (db *DB) Namespace(name string) *Namespace {
	return &Namespace{db: db, prefix: childPrefix(nil, tagNamespace, name)}
}

// Namespace returns the child namespace called name.
func (ns *Namespace) Namespace(name string) *Namespace {
	return &Namespace{db: ns.db, prefix: childPrefix(ns.prefix, tagNamespace, name)}
}

func (ns *Namespace) parentPrefix() []byte { return ns.prefix }
func (ns *Namespace) parentDB() *DB        { return ns.db }

// Drop atomically deletes every bucket and namespace inside ns.
func (ns *Namespace) Drop() error {
	return ns.db.Update(func(tx *Tx) error {
		return deleteRange(tx, ns.prefix, prefixEnd(ns.prefix))
	})
}

func deleteRange(s store, lo, hi []byte) error {
	var keys [][]byte
	if err := s.scan(lo, hi, true, func(k, _ []byte) bool {
		keys = append(keys, bytes.Clone(k))
		return true
	}); err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.del(k); err != nil {
			return err
		}
	}
	return nil
}

// BucketOption customises NewBucket.
type BucketOption func(*bucketConfig)

type bucketConfig struct {
	keyCodec, valCodec any
}

// WithKeyCodec overrides the automatically derived key codec. The codec must
// preserve ordering (see KeyCodec).
func WithKeyCodec[K any](c KeyCodec[K]) BucketOption {
	return func(cfg *bucketConfig) { cfg.keyCodec = c }
}

// WithCodec overrides the automatically derived value codec, e.g.
// WithCodec(JSON[User]()) or a hand-written zero-allocation codec.
func WithCodec[V any](c Codec[V]) BucketOption {
	return func(cfg *bucketConfig) { cfg.valCodec = c }
}

// Bucket is a typed, ordered collection of K -> V stored in a DB.
//
// Keys are encoded with an order-preserving binary encoding, so iteration
// follows the natural order of K (numbers numerically, strings
// lexicographically, structs field by field). Values are encoded with a
// codec compiled once for V. Both are derived automatically from the type
// parameters; no Marshal/Unmarshal methods are needed.
//
// A Bucket is cheap, immutable and safe for concurrent use.
type Bucket[K, V any] struct {
	db     *DB
	s      store
	prefix []byte
	end    []byte
	kc     KeyCodec[K]
	vc     Codec[V]
}

// NewBucket opens the bucket called name inside parent (a *DB or a
// *Namespace). Codecs are derived from K and V; an unsupported type is
// reported here, not on first use.
func NewBucket[K, V any](parent Parent, name string, opts ...BucketOption) (*Bucket[K, V], error) {
	var cfg bucketConfig
	for _, o := range opts {
		o(&cfg)
	}
	b := &Bucket[K, V]{
		db:     parent.parentDB(),
		s:      parent.parentDB(),
		prefix: childPrefix(parent.parentPrefix(), tagBucket, name),
	}
	b.end = prefixEnd(b.prefix)

	switch c := cfg.keyCodec.(type) {
	case nil:
		kc, err := OrderedKey[K]()
		if err != nil {
			return nil, fmt.Errorf("waffle: bucket %q: %w", name, err)
		}
		b.kc = kc
	case KeyCodec[K]:
		b.kc = c
	default:
		return nil, fmt.Errorf("waffle: bucket %q: key codec %T does not match key type %s", name, c, reflect.TypeFor[K]())
	}

	switch c := cfg.valCodec.(type) {
	case nil:
		vc, err := defaultCodec[V]()
		if err != nil {
			return nil, fmt.Errorf("waffle: bucket %q: %w", name, err)
		}
		b.vc = vc
	case Codec[V]:
		b.vc = c
	default:
		return nil, fmt.Errorf("waffle: bucket %q: codec %T does not match value type %s", name, c, reflect.TypeFor[V]())
	}
	return b, nil
}

// MustBucket is like NewBucket but panics on error. Intended for
// package-level bucket declarations.
func MustBucket[K, V any](parent Parent, name string, opts ...BucketOption) *Bucket[K, V] {
	b, err := NewBucket[K, V](parent, name, opts...)
	if err != nil {
		panic(err)
	}
	return b
}

// defaultCodec stores string- and []byte-kinded values raw (no framing) and
// compiles a codec for everything else.
func defaultCodec[V any]() (Codec[V], error) {
	t := reflect.TypeFor[V]()
	switch {
	case t.Kind() == reflect.String:
		return rawString[V]{}, nil
	case t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 &&
		!reflect.PointerTo(t).Implements(binaryUnmarshalerType):
		return rawBytes[V]{}, nil
	}
	return Compiled[V]()
}

type rawString[V any] struct{}

func (rawString[V]) Append(dst []byte, v V) ([]byte, error) {
	return append(dst, *(*string)(noescape(unsafe.Pointer(&v)))...), nil
}

func (rawString[V]) Decode(src []byte) (V, error) {
	var v V
	*(*string)(unsafe.Pointer(&v)) = string(src)
	return v, nil
}

type rawBytes[V any] struct{}

func (rawBytes[V]) Append(dst []byte, v V) ([]byte, error) {
	return append(dst, *(*[]byte)(noescape(unsafe.Pointer(&v)))...), nil
}

func (rawBytes[V]) Decode(src []byte) (V, error) {
	var v V
	*(*[]byte)(unsafe.Pointer(&v)) = bytes.Clone(src)
	return v, nil
}

// In returns a view of b bound to tx: reads see the transaction's pending
// writes and writes are committed atomically with it.
func (b *Bucket[K, V]) In(tx *Tx) *Bucket[K, V] {
	c := *b
	c.s = tx
	return &c
}

var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

func getBuf() *[]byte { return bufPool.Get().(*[]byte) }

func putBuf(p *[]byte, b []byte) {
	if cap(b) > 64<<10 {
		return // let oversized buffers go
	}
	*p = b[:0]
	bufPool.Put(p)
}

func (b *Bucket[K, V]) key(dst []byte, k K) []byte {
	return b.kc.AppendKey(append(dst[:0], b.prefix...), k)
}

// Get returns the value stored under k and whether it was found.
func (b *Bucket[K, V]) Get(k K) (v V, found bool, err error) {
	kp, vp := getBuf(), getBuf()
	key := b.key(*kp, k)
	val, found, err := b.s.get(key, *vp)
	if found && err == nil {
		v, err = b.vc.Decode(val)
	}
	putBuf(kp, key)
	putBuf(vp, val)
	return v, found, err
}

// Has reports whether k is present.
func (b *Bucket[K, V]) Has(k K) (bool, error) {
	_, found, err := b.Get(k) // TODO: key-only lookup
	return found, err
}

// Put stores v under k, replacing any previous value.
func (b *Bucket[K, V]) Put(k K, v V) error {
	kp, vp := getBuf(), getBuf()
	key := b.key(*kp, k)
	val, err := b.vc.Append(*vp, v)
	if err == nil {
		err = b.s.put(key, val)
	}
	putBuf(kp, key)
	putBuf(vp, val)
	return err
}

// Delete removes k. Deleting a missing key is not an error.
func (b *Bucket[K, V]) Delete(k K) error {
	kp := getBuf()
	key := b.key(*kp, k)
	err := b.s.del(key)
	putBuf(kp, key)
	return err
}

// Update atomically reads k, passes the current value (and whether it
// existed) to fn, and stores the result. Returning an error aborts.
func (b *Bucket[K, V]) Update(k K, fn func(v V, found bool) (V, error)) error {
	if tx, ok := b.s.(*Tx); ok {
		return b.update(tx, k, fn)
	}
	return b.db.Update(func(tx *Tx) error { return b.update(tx, k, fn) })
}

func (b *Bucket[K, V]) update(tx *Tx, k K, fn func(V, bool) (V, error)) error {
	bt := b.In(tx)
	v, found, err := bt.Get(k)
	if err != nil {
		return err
	}
	if v, err = fn(v, found); err != nil {
		return err
	}
	return bt.Put(k, v)
}

// Clear deletes every entry in the bucket atomically.
func (b *Bucket[K, V]) Clear() error {
	if tx, ok := b.s.(*Tx); ok {
		return deleteRange(tx, b.prefix, b.end)
	}
	return b.db.Update(func(tx *Tx) error { return deleteRange(tx, b.prefix, b.end) })
}

// Count returns the number of entries (by walking the keys).
func (b *Bucket[K, V]) Count() (int, error) {
	n := 0
	err := b.s.scan(b.prefix, b.end, true, func(_, _ []byte) bool { n++; return true })
	return n, err
}

// Bounds selects a key range for Scan. A nil bound is open.
type Bounds[K any] struct {
	From *K // inclusive
	To   *K // exclusive
}

// Scan calls fn for each entry within bounds, in key order, stopping at the
// first error. Returning ErrStop from fn ends the scan without an error.
// Scan is the error-returning counterpart of the iterator methods.
func (b *Bucket[K, V]) Scan(bounds Bounds[K], fn func(k K, v V) error) error {
	lo, hi := b.prefix, b.end
	if bounds.From != nil {
		lo = b.key(nil, *bounds.From)
	}
	if bounds.To != nil {
		hi = b.key(nil, *bounds.To)
	}
	var ferr error
	err := b.iterate(lo, hi, false, func(k K, v V) bool {
		ferr = fn(k, v)
		return ferr == nil
	})
	if err == nil && !errors.Is(ferr, ErrStop) {
		err = ferr
	}
	return err
}

// ErrStop can be returned from a Scan callback to end the scan early.
var ErrStop = errors.New("waffle: stop scan")

func (b *Bucket[K, V]) iterate(lo, hi []byte, keysOnly bool, fn func(K, V) bool) error {
	var derr error
	err := b.s.scan(lo, hi, keysOnly, func(kb, vb []byte) bool {
		k, err := b.kc.DecodeKey(kb[len(b.prefix):])
		if err != nil {
			derr = fmt.Errorf("decode key %x: %w", kb, err)
			return false
		}
		var v V
		if !keysOnly {
			if v, err = b.vc.Decode(vb); err != nil {
				derr = fmt.Errorf("decode value for key %x: %w", kb, err)
				return false
			}
		}
		return fn(k, v)
	})
	if err == nil {
		err = derr
	}
	return err
}

// IterError is the panic value raised by the iterator methods (All, Range,
// Prefix, Keys) when the underlying scan fails, which only happens on I/O
// errors, corrupt data, or a closed database. Use Scan to receive such
// errors as return values instead.
type IterError struct{ Err error }

func (e *IterError) Error() string { return "waffle: iteration failed: " + e.Err.Error() }
func (e *IterError) Unwrap() error { return e.Err }

func (b *Bucket[K, V]) seq(lo, hi []byte, keysOnly bool) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if err := b.iterate(lo, hi, keysOnly, yield); err != nil {
			panic(&IterError{err})
		}
	}
}

// All iterates over every entry in key order.
func (b *Bucket[K, V]) All() iter.Seq2[K, V] { return b.seq(b.prefix, b.end, false) }

// Keys iterates over every key in order without reading values.
func (b *Bucket[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		for k := range b.seq(b.prefix, b.end, true) {
			if !yield(k) {
				return
			}
		}
	}
}

// Range iterates over keys in [from, to).
func (b *Bucket[K, V]) Range(from, to K) iter.Seq2[K, V] {
	return b.seq(b.key(nil, from), b.key(nil, to), false)
}

// From iterates over keys >= from.
func (b *Bucket[K, V]) From(from K) iter.Seq2[K, V] {
	return b.seq(b.key(nil, from), b.end, false)
}

// Prefix iterates over keys sharing a prefix with partial.
//
//   - Struct keys: entries whose first `fields` fields equal those of
//     partial (e.g. all orders of one user for key struct{User, OrderID}).
//   - String / []byte keys: entries whose key starts with partial; fields is
//     ignored.
//
// It returns an error for other key types or an out-of-range field count.
func (b *Bucket[K, V]) Prefix(partial K, fields int) (iter.Seq2[K, V], error) {
	pc, ok := b.kc.(prefixKeyCodec[K])
	if !ok {
		return nil, errors.New("waffle: Prefix is not supported by a custom key codec")
	}
	lo, err := pc.appendPrefix(bytes.Clone(b.prefix), partial, fields)
	if err != nil {
		return nil, err
	}
	hi := prefixEnd(lo)
	if hi == nil {
		hi = b.end
	}
	return b.seq(lo, hi, false), nil
}

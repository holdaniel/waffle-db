package waffle

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"math"
	"reflect"
	"sync"
	"time"
	"unsafe"
)

// store is what a Bucket reads and writes through: the DB itself
// (auto-commit) or a Tx.
type store interface {
	// get returns the value and its expiry (unix ns, 0 = never).
	get(key, dst []byte) (val []byte, exp int64, found bool, err error)
	put(key, val []byte, exp int64) error
	del(key []byte) error
	delRange(lo, hi []byte) error
	scan(lo, hi []byte, o scanOpts, fn func(k, v []byte) bool) error
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

// Drop atomically deletes every bucket and namespace inside ns. It writes a
// single range-delete record, so its cost does not depend on how much data
// the namespace holds.
func (ns *Namespace) Drop() error {
	return ns.db.delRange(ns.prefix, prefixEnd(ns.prefix))
}

// BucketOption customises NewBucket.
type BucketOption func(*bucketConfig)

type bucketConfig struct {
	keyCodec, valCodec any
	ttl                time.Duration
}

// WithTTL gives every entry written with Put or Update a time-to-live, e.g.
// for idempotency keys, sessions or temporary holds. PutTTL overrides it per
// entry. Expired entries are invisible immediately; Compact and PurgeExpired
// reclaim their space.
func WithTTL(ttl time.Duration) BucketOption {
	return func(cfg *bucketConfig) { cfg.ttl = ttl }
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
	ttl    time.Duration // default TTL for Put; 0 = none
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
	if cfg.ttl < 0 {
		return nil, fmt.Errorf("waffle: bucket %q: negative TTL %v", name, cfg.ttl)
	}
	b.ttl = cfg.ttl

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

// Get returns the value stored under k and whether it was found. Expired
// entries are reported as not found.
func (b *Bucket[K, V]) Get(k K) (v V, found bool, err error) {
	kp, vp := getBuf(), getBuf()
	key := b.key(*kp, k)
	val, _, found, err := b.s.get(key, *vp)
	if found && err == nil {
		v, err = b.vc.Decode(val)
	}
	putBuf(kp, key)
	putBuf(vp, val)
	return v, found, err
}

// Has reports whether k is present (and not expired).
func (b *Bucket[K, V]) Has(k K) (bool, error) {
	_, found, err := b.Get(k) // TODO: key-only lookup
	return found, err
}

// ExpiresAt reports when k expires. The time is zero if k has no TTL;
// found is false if k is missing or already expired.
func (b *Bucket[K, V]) ExpiresAt(k K) (at time.Time, found bool, err error) {
	kp, vp := getBuf(), getBuf()
	key := b.key(*kp, k)
	val, exp, found, err := b.s.get(key, *vp)
	putBuf(kp, key)
	putBuf(vp, val)
	if found && exp != 0 {
		at = time.Unix(0, exp)
	}
	return at, found, err
}

// Put stores v under k, replacing any previous value and its TTL. If the
// bucket has a default TTL (WithTTL), the entry gets it; otherwise the entry
// never expires.
func (b *Bucket[K, V]) Put(k K, v V) error {
	return b.put(k, v, b.deadline(b.ttl))
}

// PutTTL stores v under k so that it expires after ttl.
func (b *Bucket[K, V]) PutTTL(k K, v V, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("waffle: TTL must be positive, got %v", ttl)
	}
	return b.put(k, v, b.deadline(ttl))
}

// deadline converts a TTL into an absolute expiry (0 = never).
func (b *Bucket[K, V]) deadline(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	now := b.db.now()
	if exp := now + int64(ttl); exp > now {
		return exp
	}
	return math.MaxInt64 // overflow: effectively never
}

func (b *Bucket[K, V]) put(k K, v V, exp int64) error {
	kp, vp := getBuf(), getBuf()
	key := b.key(*kp, k)
	val, err := b.vc.Append(*vp, v)
	if err == nil {
		err = b.s.put(key, val, exp)
	}
	putBuf(kp, key)
	putBuf(vp, val)
	return err
}

// Expire sets k to expire after ttl, keeping its value, e.g. to extend a
// session on activity. It reports whether k existed.
func (b *Bucket[K, V]) Expire(k K, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("waffle: TTL must be positive, got %v", ttl)
	}
	found := false
	err := b.inTx(func(tx *Tx) error {
		key := b.key(nil, k)
		val, _, ok, err := tx.get(key, nil)
		if err != nil || !ok {
			return err
		}
		found = true
		return tx.put(key, val, b.deadline(ttl))
	})
	return found, err
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
//
// An existing entry keeps its expiry, like Redis INCR, so a counter with a
// TTL (e.g. a rate-limit window) still expires on schedule. A new entry gets
// the bucket's default TTL, if any.
func (b *Bucket[K, V]) Update(k K, fn func(v V, found bool) (V, error)) error {
	return b.inTx(func(tx *Tx) error {
		key := b.key(nil, k)
		val, exp, found, err := tx.get(key, nil)
		if err != nil {
			return err
		}
		var v V
		if found {
			if v, err = b.vc.Decode(val); err != nil {
				return err
			}
		} else {
			exp = b.deadline(b.ttl)
		}
		if v, err = fn(v, found); err != nil {
			return err
		}
		enc, err := b.vc.Append(nil, v)
		if err != nil {
			return err
		}
		return tx.put(key, enc, exp)
	})
}

// inTx runs fn in b's transaction, or in a new one if b is not bound to one.
func (b *Bucket[K, V]) inTx(fn func(tx *Tx) error) error {
	if tx, ok := b.s.(*Tx); ok {
		return fn(tx)
	}
	return b.db.Update(fn)
}

// Clear deletes every entry in the bucket atomically. It writes a single
// range-delete record, so its cost does not depend on the bucket's size.
func (b *Bucket[K, V]) Clear() error {
	return b.s.delRange(b.prefix, b.end)
}

// Count returns the number of entries (by walking the keys).
func (b *Bucket[K, V]) Count() (int, error) {
	n := 0
	err := b.s.scan(b.prefix, b.end, scanOpts{keysOnly: true}, func(_, _ []byte) bool { n++; return true })
	return n, err
}

// Bounds selects a key range and direction for Scan. A nil bound is open.
type Bounds[K any] struct {
	From    *K   // inclusive
	To      *K   // exclusive
	Reverse bool // descending key order
}

// Scan calls fn for each entry within bounds, in key order (descending if
// bounds.Reverse), stopping at the first error. Returning ErrStop from fn
// ends the scan without an error.
func (b *Bucket[K, V]) Scan(bounds Bounds[K], fn func(k K, v V) error) error {
	lo, hi := b.prefix, b.end
	if bounds.From != nil {
		lo = b.key(nil, *bounds.From)
	}
	if bounds.To != nil {
		hi = b.key(nil, *bounds.To)
	}
	var ferr error
	err := b.iterate(lo, hi, scanOpts{reverse: bounds.Reverse}, func(k K, v V) bool {
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

func (b *Bucket[K, V]) iterate(lo, hi []byte, o scanOpts, fn func(K, V) bool) error {
	var derr error
	err := b.s.scan(lo, hi, o, func(kb, vb []byte) bool {
		k, err := b.kc.DecodeKey(kb[len(b.prefix):])
		if err != nil {
			derr = fmt.Errorf("decode key %x: %w", kb, err)
			return false
		}
		var v V
		if !o.keysOnly {
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

// Iter is a range over a bucket. Range over Seq (or Keys), then check Err,
// the same way you would use bufio.Scanner:
//
//	it := users.All()
//	for id, u := range it.Seq() {
//		...
//	}
//	if err := it.Err(); err != nil {
//		...
//	}
//
// Iteration is in ascending key order unless Reverse is called. It stops at
// the first error (I/O failure, corrupt data, a closed database or a
// finished transaction); it never panics. Expired entries are skipped. An
// Iter can be ranged over more than once; each pass re-reads the data. It is
// not safe for concurrent use.
type Iter[K, V any] struct {
	b        *Bucket[K, V]
	lo, hi   []byte
	reverse  bool
	setupErr error
	err      error
}

// Reverse switches the iterator to descending key order and returns it, so
// it can be chained: users.All().Reverse(). The key range is unchanged; for
// Range(from, to) it yields to-exclusive down to from-inclusive.
func (it *Iter[K, V]) Reverse() *Iter[K, V] {
	it.reverse = true
	return it
}

// First returns the first entry in iteration order: the smallest key, or the
// largest after Reverse. For example, the latest event of an order:
//
//	k, ev, ok, err := events.Prefix(EventKey{OrderID: id}, 1).Reverse().First()
func (it *Iter[K, V]) First() (k K, v V, found bool, err error) {
	for k, v = range it.Seq() {
		return k, v, true, nil
	}
	var zk K
	var zv V
	return zk, zv, false, it.Err()
}

// Seq yields each entry in key order.
func (it *Iter[K, V]) Seq() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if it.setupErr != nil {
			return
		}
		it.err = it.b.iterate(it.lo, it.hi, scanOpts{reverse: it.reverse}, yield)
	}
}

// Keys yields each key in order without reading or decoding values.
func (it *Iter[K, V]) Keys() iter.Seq[K] {
	return func(yield func(K) bool) {
		if it.setupErr != nil {
			return
		}
		it.err = it.b.iterate(it.lo, it.hi, scanOpts{keysOnly: true, reverse: it.reverse}, func(k K, _ V) bool { return yield(k) })
	}
}

// Err returns the error that ended the most recent pass, if any.
func (it *Iter[K, V]) Err() error {
	if it.setupErr != nil {
		return it.setupErr
	}
	return it.err
}

func (b *Bucket[K, V]) newIter(lo, hi []byte) *Iter[K, V] {
	return &Iter[K, V]{b: b, lo: lo, hi: hi}
}

// All ranges over every entry in key order.
func (b *Bucket[K, V]) All() *Iter[K, V] { return b.newIter(b.prefix, b.end) }

// Range ranges over keys in [from, to).
func (b *Bucket[K, V]) Range(from, to K) *Iter[K, V] {
	return b.newIter(b.key(nil, from), b.key(nil, to))
}

// From ranges over keys >= from.
func (b *Bucket[K, V]) From(from K) *Iter[K, V] {
	return b.newIter(b.key(nil, from), b.end)
}

// Prefix ranges over keys sharing a prefix with partial.
//
//   - Struct keys: entries whose first `fields` fields equal those of
//     partial (e.g. all orders of one user for key struct{User, OrderID}).
//   - String / []byte keys: entries whose key starts with partial; fields is
//     ignored.
//
// For other key types or an out-of-range field count, the returned Iter
// yields nothing and its Err reports why.
func (b *Bucket[K, V]) Prefix(partial K, fields int) *Iter[K, V] {
	pc, ok := b.kc.(prefixKeyCodec[K])
	if !ok {
		return &Iter[K, V]{setupErr: errors.New("waffle: Prefix is not supported by a custom key codec")}
	}
	lo, err := pc.appendPrefix(bytes.Clone(b.prefix), partial, fields)
	if err != nil {
		return &Iter[K, V]{setupErr: err}
	}
	hi := prefixEnd(lo)
	if hi == nil {
		hi = b.end
	}
	return b.newIter(lo, hi)
}

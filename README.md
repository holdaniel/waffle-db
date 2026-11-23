# waffle-db

A generics-native, zero-boilerplate embedded key-value store for Go. Pure Go, standard library only.

```go
db, _ := waffle.Open("app.db", nil)
defer db.Close()

users := waffle.MustBucket[uint64, User](db, "users")

users.Put(42, User{Name: "Ada"})
u, found, err := users.Get(42)

for id, u := range users.All() { // ordered by id
    fmt.Println(id, u.Name)
}
```

You don't write `MarshalBinary`/`UnmarshalBinary`, and you don't pay for `encoding/json` on every call.

## Design

The core problem: Go's type safety exists only at compile time, but disk stores bytes. waffle bridges the two in three layers.

### 1. Order-preserving key encoding

Keys are encoded so that **byte order equals logical order**. Ranges and prefix scans then work on typed keys.

| Key kind | Encoding |
|---|---|
| `uint*` | big-endian, fixed width |
| `int*` | big-endian with sign bit flipped |
| `float*` | IEEE bits; negatives fully inverted, positives sign-flipped |
| `string`, `[]byte` | `0x00` escaped as `00 FF`, terminated by `00 01` (self-delimiting and order-safe) |
| `time.Time` | sign-flipped seconds + nanos (full range, decodes as UTC) |
| struct | fields concatenated in declaration order (a composite key) |

Named types (`type UserID uint64`) and nested structs work too. Struct keys give you multi-column indexes for free:

```go
type OrderKey struct { Customer string; At time.Time }
orders := waffle.MustBucket[OrderKey, Order](db, "orders")

seq, _ := orders.Prefix(OrderKey{Customer: "alice"}, 1) // all of alice's orders, by time
for k, o := range seq { ... }
```

### 2. Compiled, zero-allocation value codecs

The first time a bucket is created for a type `V`, reflection walks `V` **once** and builds a tree of closures that read and write raw memory (`unsafe.Pointer` + field offsets). Encoding and decoding then do no reflection (maps are the one exception). Encoding into the pooled buffers the bucket provides doesn't allocate.

* Supported types: primitives, strings, slices, arrays, maps, pointers, nested and recursive structs, and anything implementing `encoding.BinaryAppender` + `BinaryUnmarshaler` (`time.Time`, `netip.Addr`, …).
* An unsupported type (an interface, chan or func) is rejected when `NewBucket` is called, not at runtime.
* Schema evolution: appending fields to a struct is backward compatible. Old records decode, and the new fields stay at their zero value. Removing or reordering fields isn't supported, and data written by a newer schema fails loudly instead of being silently truncated.
* Codecs are pluggable per bucket: `WithCodec(waffle.JSON[User]())`, `WithCodec(waffle.CodecFunc(...))`, or your own `Codec[V]`.

### 3. Storage engine

* A single **append-only log** of CRC32-C checked frames. Each frame holds one commit.
* An in-memory **skiplist index** maps each key to the offset of its value. Values stay on disk and are read with `pread`.
* **Atomic transactions:** `db.Update(fn)` buffers writes, with read-your-writes (including inside scans), and commits them as one frame.
* **Crash recovery:** on open, the log is replayed and everything after the first torn or corrupt frame is truncated (`Stats().TruncatedBytes`).
* **Compaction:** `db.Compact()` rewrites only live data to a temp file, fsyncs it and renames it into place. Readers keep running meanwhile.
* **Concurrency:** many readers and one writer. Iteration reads in chunks and releases the lock between them, so you can write while you iterate without deadlocking.

### Layered key space

```go
tenant := db.Namespace("tenants").Namespace("acme")
users  := waffle.MustBucket[string, User](tenant, "users")
tenant.Drop() // atomically delete everything for acme
```

Each namespace or bucket name becomes a tagged, escaped segment of the key prefix. Sibling names can't collide, and a bucket never collides with a namespace of the same name.

## API at a glance

```go
b.Get(k) (V, bool, error)      b.Put(k, v) error           b.Delete(k) error
b.Has(k)                       b.Update(k, func(V, bool) (V, error)) error
b.All() / b.Keys()             b.Range(from, to) / b.From(k)
b.Prefix(partial, nFields)     b.Scan(Bounds[K]{From, To}, fn) error
b.Count()  b.Clear()           b.In(tx)

db.Update(func(tx *waffle.Tx) error)   db.Compact()   db.Stats()   db.Sync()   db.Close()
waffle.OpenMemory()                    // same engine, in RAM: tests and caches
```

The iterator methods (`All`, `Range`, …) panic with `*IterError` on I/O errors or corruption. Use `Scan` to get those errors back as return values.

## Benchmarks (Apple M-series, Go 1.25)

```
CodecCompiledAppend   38 ns/op     0 B/op   0 allocs/op
CodecJSONAppend      306 ns/op   192 B/op   2 allocs/op
CodecCompiledDecode  180 ns/op   104 B/op   6 allocs/op   (string fields must be allocated)
CodecJSONDecode     1260 ns/op   400 B/op  12 allocs/op
KeyEncodeStruct       14 ns/op     0 B/op   0 allocs/op
Get (uint64→uint64)  666 ns/op     0 B/op   0 allocs/op   (file-backed, pread)
Put (uint64→uint64) 2124 ns/op     1 B/op   0 allocs/op   (no fsync)
```

Run them with `go test -bench=. -benchmem`.

## Limits and roadmap

* The whole key set lives in memory (a Bitcask-style trade-off), so values can be much larger than RAM but keys can't. The natural next step is a paged B+tree or LSM index.
* One process per file. There is no file lock yet (an `flock` is easy to add on Unix).
* Iteration is forward only and not a point-in-time snapshot.
* Not done yet: reverse iteration, secondary indexes (`Index[V, IK]` derived from values), automatic compaction, and a `go generate` codec generator to remove the remaining closure-call overhead.

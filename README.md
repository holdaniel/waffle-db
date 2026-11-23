# waffle-db

A generics-native, zero-boilerplate embedded key-value store for Go. Pure Go, standard library only.

```go
db, _ := waffle.Open("app.db", nil)
defer db.Close()

users := waffle.MustBucket[uint64, User](db, "users")

users.Put(42, User{Name: "Ada"})
u, found, err := users.Get(42)

it := users.All()
for id, u := range it.Seq() { // ordered by id
    fmt.Println(id, u.Name)
}
if err := it.Err(); err != nil { ... }
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
| `float*` | IEEE bits; negatives fully inverted, positives sign-flipped. `-0` is stored as `+0` and every NaN as one NaN, which sorts after `+Inf` |
| `string`, `[]byte` | `0x00` escaped as `00 FF`, terminated by `00 01` (self-delimiting and order-safe) |
| `time.Time` | sign-flipped seconds + nanos (full range, decodes as UTC) |
| struct | fields concatenated in declaration order (a composite key) |

Named types (`type UserID uint64`) and nested structs work too. Struct keys give you multi-column indexes for free:

```go
type OrderKey struct { Customer string; At time.Time }
orders := waffle.MustBucket[OrderKey, Order](db, "orders")

it := orders.Prefix(OrderKey{Customer: "alice"}, 1) // all of alice's orders, by time
for k, o := range it.Seq() { ... }

// newest first, or just the latest one
for k, o := range orders.Prefix(OrderKey{Customer: "alice"}, 1).Reverse().Seq() { ... }
k, latest, ok, err := orders.Prefix(OrderKey{Customer: "alice"}, 1).Reverse().First()
```

Every field of a key struct must be part of the key. An unexported field is rejected when the bucket is created, since two keys that differ only in that field would collide. Tag it `waffle:"-"` to leave it out on purpose.

### 2. Compiled, zero-allocation value codecs

The first time a bucket is created for a type `V`, reflection walks `V` **once** and builds a tree of closures that read and write raw memory (`unsafe.Pointer` + field offsets). Encoding and decoding then do no reflection (maps are the one exception). Encoding into the pooled buffers the bucket provides doesn't allocate.

* Supported types: primitives, strings, slices, arrays, maps, pointers, nested and recursive structs, and any type with its own binary encoding (`BinaryAppender` or `BinaryMarshaler`, plus `BinaryUnmarshaler`), such as `time.Time` and `netip.Addr`.
* Structs encode their exported fields plus embedded structs, as `encoding/json` does. A type whose data is all in unexported fields (`big.Int`, decimal types) uses its `TextMarshaler` if it has one, and is rejected otherwise. It is never silently stored as empty.
* An unsupported type (an interface, chan or func) is rejected when `NewBucket` is called, not at runtime.
* Schema evolution: appending fields to a struct is backward compatible. Old records decode, and the new fields stay at their zero value. Removing or reordering fields isn't supported, and data written by a newer schema fails loudly instead of being silently truncated.
* Codecs are pluggable per bucket: `WithCodec(waffle.JSON[User]())`, `WithCodec(waffle.CodecFunc(...))`, or your own `Codec[V]`.

### 3. Storage engine

* A single **append-only log** of frames. Each frame holds one commit and carries two CRC32-C checksums: one for the header, one for the payload.
* An in-memory **skiplist index** maps each key to the offset of its value. Values stay on disk and are read with `pread`.
* **Atomic transactions:** `db.Update(fn)` buffers writes, with read-your-writes (including inside scans), and commits them as one frame.
* **Crash recovery:** on open, the log is replayed. A damaged frame with nothing intact after it is a torn tail from a crash, and is truncated (`Stats().TruncatedBytes`). A damaged frame with intact frames after it is real corruption: `Open` returns a `*CorruptionError` and leaves the file untouched instead of throwing away the later commits. `Options{TruncateCorrupt: true}` salvages it explicitly.
* **Failed writes:** if a write or fsync fails, the commit is rolled back out of the file, so a commit that returned an error can't reappear after a restart. After a failed fsync the database also refuses further writes (`ErrFailed`) until it is reopened. Reads keep working.
* **One handle per file:** `Open` takes an exclusive `flock`, so a second open of the same file fails with `ErrLocked`, in the same process or another. The lock carries over to the new file when `Compact` replaces it. (On Windows there's no lock yet.)
* **Compaction:** `db.Compact()` rewrites only live data to a temp file, fsyncs it and renames it into place. Readers keep running meanwhile.
* **Bidirectional:** the skiplist keeps backward links, so any range can be iterated in descending order at the same cost as ascending.
* **Concurrency:** many readers and one writer. Iteration reads in chunks (at most 128 entries or 1 MiB) and releases the lock between them, so you can write while you iterate without deadlocking, and large values can't exhaust memory.
* **Range deletes:** `Clear` and `Namespace.Drop` write a single range-delete record. Their cost doesn't depend on how much data they remove.

### TTL (expiring entries)

```go
idem := waffle.MustBucket[string, Result](db, "idempotency", waffle.WithTTL(24*time.Hour))
idem.Put(requestID, res)                      // expires in 24h
sessions.PutTTL(id, sess, 30*time.Minute)      // per-entry TTL
sessions.Expire(id, 30*time.Minute)            // sliding expiration, value unchanged
at, ok, _ := sessions.ExpiresAt(id)
hits.Update(user, func(n int, _ bool) (int, error) { return n + 1, nil }) // keeps the existing expiry
```

Expiry is **lazy**. There is no background goroutine or scheduler:

* Each entry stores an absolute expiry time (unix ns) in its log record and in the in-memory index.
* Every read path (`Get`, `Has`, iteration, `Count`, transactions) compares it with the clock, so an entry is invisible from the instant it expires.
* Nothing is written when an entry expires. On replay, an expired put counts as a delete, so it can't resurrect an older value of the same key.
* Space is reclaimed on demand: `db.PurgeExpired()` frees index memory, and `db.Compact()` drops expired records from disk. Call them from your own ticker if you want periodic cleanup.
* `Options.Now` replaces the clock, for testing expiry without sleeping.
* `Put` replaces the TTL (a bucket without `WithTTL` clears it). `Update` keeps the existing expiry, like Redis `INCR`, so rate-limit windows don't slide.
* If the system clock jumps backwards past an entry's expiry, the entry becomes visible again until the clock catches up.

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
b.All() / b.Range(from, to) / b.From(k) / b.Prefix(partial, nFields)   // -> *Iter
it.Seq() (keys and values)     it.Keys() (keys only)       it.Err()
it.Reverse() (descending)      it.First() (K, V, bool, error)
b.PutTTL(k, v, ttl)  b.Expire(k, ttl)  b.ExpiresAt(k)       WithTTL(d) bucket option
b.Scan(Bounds[K]{From, To}, fn) error
b.Count()  b.Clear()           b.In(tx)

db.Update(func(tx *waffle.Tx) error)   db.Compact()   db.PurgeExpired()   db.Stats()   db.Close()
waffle.Open(waffle.MemoryPath, opts) / waffle.OpenMemory()   // same engine, in RAM
```

Iteration never panics. It stops at the first error (I/O failure, corrupt data, a closed database or a finished transaction), and `it.Err()` reports it, the same way `bufio.Scanner` does.

## Benchmarks (Apple M-series, Go 1.25)

```
CodecCompiledAppend   38 ns/op     0 B/op   0 allocs/op
CodecJSONAppend      306 ns/op   192 B/op   2 allocs/op
CodecCompiledDecode  180 ns/op   104 B/op   6 allocs/op   (string fields must be allocated)
CodecJSONDecode     1260 ns/op   400 B/op  12 allocs/op
KeyEncodeStruct       14 ns/op     0 B/op   0 allocs/op
Get (uint64→uint64)  668 ns/op     0 B/op   0 allocs/op   (file-backed, pread)
Put (uint64→uint64) 2350 ns/op     2 B/op   0 allocs/op   (no fsync; PutTTL is the same)
Iterate 10k, fwd    370 µs/op                            (reverse: 375 µs/op)
```

Run them with `go test -bench=. -benchmem`.

## Limits and roadmap

* The whole key set lives in memory (a Bitcask-style trade-off), so values can be much larger than RAM but keys can't. The natural next step is a paged B+tree or LSM index.
* File locking is Unix-only; Windows needs `LockFileEx`.
* Without `SyncWrites`, a power loss can leave a hole in the log with later frames intact, because the OS may reorder page writes. `Open` then reports corruption rather than guessing, and `TruncateCorrupt` recovers it.
* Iteration is not a point-in-time snapshot.
* Not done yet: secondary indexes (`Index[V, IK]` derived from values), a change feed (`Watch`), optional background purge and compaction, and a `go generate` codec generator to remove the remaining closure-call overhead.

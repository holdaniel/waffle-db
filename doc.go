// Package waffle is a generics-native, zero-boilerplate embedded key-value
// store.
//
// You declare typed collections and use them; there is no Marshal/Unmarshal
// glue to write and no encoding/json on the hot path:
//
//	db, _ := waffle.Open("app.db", nil)
//	users := waffle.MustBucket[uint64, User](db, "users")
//
//	users.Put(42, User{Name: "Ada"})
//	u, ok, _ := users.Get(42)
//	it := users.All()
//	for id, u := range it.Seq() { ... }
//	if err := it.Err(); err != nil { ... }
//
// Design in three layers:
//
//   - Keys are encoded with an order-preserving binary encoding (big-endian
//     integers with flipped sign bits, sortable floats, escaped+terminated
//     strings, field-by-field structs). Byte order equals logical order, so
//     ranges and prefix scans work on typed keys, including composite
//     struct keys.
//   - Values are encoded by a codec compiled once per type: reflection walks
//     the type a single time and produces a tree of closures over raw memory.
//     Encoding then runs without reflection and, with pooled buffers, without
//     allocation. Codecs are pluggable per bucket (WithCodec).
//   - Storage is a single append-only log of CRC-checked frames with an
//     in-memory ordered index of keys to value offsets. Each commit is one
//     frame, so transactions are atomic and crash recovery is a replay that
//     drops a torn tail (but refuses to discard data after mid-file
//     corruption). Compact reclaims space from overwritten data.
//
// Buckets live in a DB or in a Namespace; namespaces nest, giving each
// tenant or subsystem its own isolated key space that can be dropped
// atomically.
package waffle

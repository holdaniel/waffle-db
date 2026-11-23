package waffle

import (
	"bytes"
	"slices"
	"strings"
	"unsafe"
)

// Tx is a read-write transaction, obtained from DB.Update. Bind buckets to it
// with Bucket.In. Reads through the Tx see its own uncommitted writes.
type Tx struct {
	db     *DB
	writes map[string]txWrite // point writes made after the last overlapping range delete
	ranges []keyRange         // range deletes, in order
	done   bool
}

type txWrite struct {
	val []byte
	del bool
	exp int64 // expiry for puts, unix ns; 0 = never
}

type keyRange struct{ lo, hi []byte } // [lo, hi), hi == nil: unbounded

func (r keyRange) contains(k []byte) bool {
	return bytes.Compare(k, r.lo) >= 0 && (r.hi == nil || bytes.Compare(k, r.hi) < 0)
}

// rangeDeleted reports whether a committed key is hidden by a range delete
// in this transaction.
func (tx *Tx) rangeDeleted(k []byte) bool {
	for _, r := range tx.ranges {
		if r.contains(k) {
			return true
		}
	}
	return false
}

func (tx *Tx) get(key, dst []byte) ([]byte, int64, bool, error) {
	if tx.done {
		return dst, 0, false, ErrTxDone
	}
	if w, ok := tx.writes[string(key)]; ok {
		if w.del || expired(w.exp, tx.db.now()) {
			return dst, 0, false, nil
		}
		return append(dst[:0], w.val...), w.exp, true, nil
	}
	if tx.rangeDeleted(key) {
		return dst, 0, false, nil
	}
	return tx.db.get(key, dst)
}

func (tx *Tx) put(key, val []byte, exp int64) error {
	if tx.done {
		return ErrTxDone
	}
	tx.writes[string(key)] = txWrite{val: bytes.Clone(val), exp: exp}
	return nil
}

func (tx *Tx) del(key []byte) error {
	if tx.done {
		return ErrTxDone
	}
	tx.writes[string(key)] = txWrite{del: true}
	return nil
}

func (tx *Tx) delRange(lo, hi []byte) error {
	if tx.done {
		return ErrTxDone
	}
	r := keyRange{bytes.Clone(lo), bytes.Clone(hi)}
	// Earlier point writes in the range are superseded. Anything written
	// after this call lands in tx.writes and is applied after the range
	// delete at commit, so it survives.
	for k := range tx.writes {
		if r.contains(unsafe.Slice(unsafe.StringData(k), len(k))) {
			delete(tx.writes, k)
		}
	}
	tx.ranges = append(tx.ranges, r)
	return nil
}

// scan merges the transaction's pending writes into the committed data, in
// either direction.
func (tx *Tx) scan(lo, hi []byte, o scanOpts, fn func(k, v []byte) bool) error {
	if tx.done {
		return ErrTxDone
	}
	var pend []string
	for k := range tx.writes {
		if k >= string(lo) && (hi == nil || k < string(hi)) {
			pend = append(pend, k)
		}
	}
	slices.Sort(pend)
	if o.reverse {
		slices.Reverse(pend)
	}
	// before reports whether pending key p comes before committed key k in
	// iteration order.
	before := func(p string, k []byte) bool {
		c := strings.Compare(p, string(k))
		if o.reverse {
			return c > 0
		}
		return c < 0
	}

	now := tx.db.now()
	i, stopped := 0, false
	// emit yields a pending write; returns false if iteration must stop.
	emit := func(k string) bool {
		w := tx.writes[k]
		if w.del || expired(w.exp, now) {
			return true
		}
		if !fn(unsafe.Slice(unsafe.StringData(k), len(k)), w.val) {
			stopped = true
			return false
		}
		return true
	}
	err := tx.db.scan(lo, hi, o, func(k, v []byte) bool {
		for i < len(pend) && before(pend[i], k) {
			if !emit(pend[i]) {
				return false
			}
			i++
		}
		if i < len(pend) && pend[i] == string(k) {
			i++
			return emit(pend[i-1]) // pending write shadows committed value
		}
		if tx.rangeDeleted(k) {
			return true
		}
		if !fn(k, v) {
			stopped = true
			return false
		}
		return true
	})
	if err != nil || stopped {
		return err
	}
	for ; i < len(pend); i++ {
		if !emit(pend[i]) {
			return nil
		}
	}
	return nil
}

// ops returns the commit's operations: range deletes first (in order), then
// the point writes, which by construction all happened after any range
// delete that covers them.
func (tx *Tx) ops() []writeOp {
	keys := make([]string, 0, len(tx.writes))
	for k := range tx.writes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	ops := make([]writeOp, 0, len(tx.ranges)+len(keys))
	for _, r := range tx.ranges {
		ops = append(ops, writeOp{op: opDelRange, key: r.lo, val: r.hi})
	}
	for _, k := range keys {
		w := tx.writes[k]
		op := writeOp{op: opPut, key: []byte(k), val: w.val, exp: w.exp}
		if w.del {
			op.op = opDel
		}
		ops = append(ops, op)
	}
	return ops
}

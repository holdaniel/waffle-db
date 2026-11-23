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

func (tx *Tx) get(key, dst []byte) ([]byte, bool, error) {
	if tx.done {
		return dst, false, ErrTxDone
	}
	if w, ok := tx.writes[string(key)]; ok {
		if w.del {
			return dst, false, nil
		}
		return append(dst[:0], w.val...), true, nil
	}
	if tx.rangeDeleted(key) {
		return dst, false, nil
	}
	return tx.db.get(key, dst)
}

func (tx *Tx) put(key, val []byte) error {
	if tx.done {
		return ErrTxDone
	}
	tx.writes[string(key)] = txWrite{val: bytes.Clone(val)}
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

// scan merges the transaction's pending writes into the committed data.
func (tx *Tx) scan(lo, hi []byte, keysOnly bool, fn func(k, v []byte) bool) error {
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

	i, stopped := 0, false
	// emit yields a pending write; returns false if iteration must stop.
	emit := func(k string) bool {
		w := tx.writes[k]
		if w.del {
			return true
		}
		if !fn(unsafe.Slice(unsafe.StringData(k), len(k)), w.val) {
			stopped = true
			return false
		}
		return true
	}
	err := tx.db.scan(lo, hi, keysOnly, func(k, v []byte) bool {
		for i < len(pend) && strings.Compare(pend[i], string(k)) < 0 {
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
		op := writeOp{op: opPut, key: []byte(k), val: w.val}
		if w.del {
			op.op = opDel
		}
		ops = append(ops, op)
	}
	return ops
}

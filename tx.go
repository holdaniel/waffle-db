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
	writes map[string]txWrite
	done   bool
}

type txWrite struct {
	val []byte
	del bool
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

func (tx *Tx) ops() []writeOp {
	keys := make([]string, 0, len(tx.writes))
	for k := range tx.writes {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	ops := make([]writeOp, len(keys))
	for i, k := range keys {
		w := tx.writes[k]
		ops[i] = writeOp{key: []byte(k), val: w.val, del: w.del}
	}
	return ops
}

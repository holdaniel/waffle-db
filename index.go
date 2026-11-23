package waffle

import "bytes"

// index is an ordered in-memory map from key to the on-disk location of the
// key's latest value. It is a skiplist with backward links on the bottom
// level, so it can be walked in both directions; values themselves stay on
// disk (or in the memory log) and are fetched with ReadAt.
//
// Not safe for concurrent mutation: the DB serialises writers and guards
// readers with an RWMutex.
type index struct {
	head  node
	level int
	rnd   uint64
	n     int
	live  int64 // approximate bytes of live records
}

const maxLevel = 24

type node struct {
	key  []byte
	off  int64
	vlen uint32
	exp  int64 // expiry, unix nanoseconds; 0 = never
	prev *node // bottom-level predecessor; nil for the first node
	next []*node
}

// expired reports whether an entry with expiry exp is dead at time now.
func expired(exp, now int64) bool { return exp != 0 && now >= exp }

func newIndex() *index {
	return &index{head: node{next: make([]*node, maxLevel)}, level: 1, rnd: 0x9E3779B97F4A7C15}
}

func (ix *index) randomLevel() int {
	// xorshift64; promote with p = 1/4.
	ix.rnd ^= ix.rnd << 13
	ix.rnd ^= ix.rnd >> 7
	ix.rnd ^= ix.rnd << 17
	lvl, r := 1, ix.rnd
	for lvl < maxLevel && r&3 == 0 {
		lvl++
		r >>= 2
	}
	return lvl
}

// seek returns the first node with key >= key, filling preds if non-nil.
func (ix *index) seek(key []byte, preds *[maxLevel]*node) *node {
	x := &ix.head
	for i := ix.level - 1; i >= 0; i-- {
		for nx := x.next[i]; nx != nil && bytes.Compare(nx.key, key) < 0; nx = x.next[i] {
			x = nx
		}
		if preds != nil {
			preds[i] = x
		}
	}
	return x.next[0]
}

// seekLT returns the last node with key < key, or the last node overall if
// key is nil. It returns nil if there is no such node.
func (ix *index) seekLT(key []byte) *node {
	x := &ix.head
	for i := ix.level - 1; i >= 0; i-- {
		for nx := x.next[i]; nx != nil && (key == nil || bytes.Compare(nx.key, key) < 0); nx = x.next[i] {
			x = nx
		}
	}
	if x == &ix.head {
		return nil
	}
	return x
}

func (ix *index) first() *node { return ix.head.next[0] }

func (ix *index) get(key []byte) *node {
	n := ix.seek(key, nil)
	if n != nil && bytes.Equal(n.key, key) {
		return n
	}
	return nil
}

func recordSize(klen int, vlen uint32) int64 { return int64(klen) + int64(vlen) + 8 }

// set inserts or updates key. The key is copied on insert.
func (ix *index) set(key []byte, off int64, vlen uint32, exp int64) {
	var preds [maxLevel]*node
	n := ix.seek(key, &preds)
	if n != nil && bytes.Equal(n.key, key) {
		ix.live += int64(vlen) - int64(n.vlen)
		n.off, n.vlen, n.exp = off, vlen, exp
		return
	}
	lvl := ix.randomLevel()
	if lvl > ix.level {
		for i := ix.level; i < lvl; i++ {
			preds[i] = &ix.head
		}
		ix.level = lvl
	}
	nn := &node{key: bytes.Clone(key), off: off, vlen: vlen, exp: exp, next: make([]*node, lvl)}
	for i := range lvl {
		nn.next[i] = preds[i].next[i]
		preds[i].next[i] = nn
	}
	if preds[0] != &ix.head {
		nn.prev = preds[0]
	}
	if nx := nn.next[0]; nx != nil {
		nx.prev = nn
	}
	ix.n++
	ix.live += recordSize(len(key), vlen)
}

// anyInRange reports whether some key lies in [lo, hi) (hi == nil: unbounded).
func (ix *index) anyInRange(lo, hi []byte) bool {
	n := ix.seek(lo, nil)
	return n != nil && (hi == nil || bytes.Compare(n.key, hi) < 0)
}

// deleteRange removes every key in [lo, hi) (hi == nil: unbounded).
func (ix *index) deleteRange(lo, hi []byte) {
	var preds [maxLevel]*node
	for {
		n := ix.seek(lo, &preds)
		if n == nil || (hi != nil && bytes.Compare(n.key, hi) >= 0) {
			return
		}
		ix.unlink(n, &preds)
	}
}

func (ix *index) delete(key []byte) bool {
	var preds [maxLevel]*node
	n := ix.seek(key, &preds)
	if n == nil || !bytes.Equal(n.key, key) {
		return false
	}
	ix.unlink(n, &preds)
	return true
}

func (ix *index) unlink(n *node, preds *[maxLevel]*node) {
	for i := range len(n.next) {
		if preds[i].next[i] == n {
			preds[i].next[i] = n.next[i]
		}
	}
	if nx := n.next[0]; nx != nil {
		nx.prev = n.prev
	}
	for ix.level > 1 && ix.head.next[ix.level-1] == nil {
		ix.level--
	}
	ix.n--
	ix.live -= recordSize(len(n.key), n.vlen)
}

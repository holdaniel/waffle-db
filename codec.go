package waffle

import (
	"encoding"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sync"
	"unsafe"
)

// Codec converts values to and from bytes for storage.
//
// Append appends the encoding of v to dst and returns the extended slice.
// Implementations should not allocate when dst has enough capacity; the
// Bucket hands in pooled buffers so steady-state writes are allocation-free.
type Codec[T any] interface {
	Append(dst []byte, v T) ([]byte, error)
	Decode(src []byte) (T, error)
}

// CodecFunc adapts a pair of functions into a Codec.
func CodecFunc[T any](appendFn func(dst []byte, v T) ([]byte, error), decodeFn func(src []byte) (T, error)) Codec[T] {
	return funcCodec[T]{appendFn, decodeFn}
}

type funcCodec[T any] struct {
	a func([]byte, T) ([]byte, error)
	d func([]byte) (T, error)
}

func (c funcCodec[T]) Append(dst []byte, v T) ([]byte, error) { return c.a(dst, v) }
func (c funcCodec[T]) Decode(src []byte) (T, error)           { return c.d(src) }

// JSON returns a codec backed by encoding/json. It is never chosen
// automatically; use it explicitly when you need a self-describing format
// (e.g. data shared with other languages) and can afford the overhead.
func JSON[T any]() Codec[T] { return jsonCodec[T]{} }

type jsonCodec[T any] struct{}

func (jsonCodec[T]) Append(dst []byte, v T) ([]byte, error) {
	b, err := json.Marshal(v)
	return append(dst, b...), err
}

func (jsonCodec[T]) Decode(src []byte) (T, error) {
	var v T
	err := json.Unmarshal(src, &v)
	return v, err
}

// Compiled returns the reflection-free value codec for T. The type is walked
// once with reflection to build a tree of encode/decode closures that operate
// on raw memory; after that, encoding and decoding do no reflection (except
// for maps).
//
// Encoding rules:
//   - bool: 1 byte; uints: uvarint; ints: zigzag varint; floats: fixed LE
//   - string, []byte: uvarint length + bytes
//   - slices, maps: uvarint (len+1), 0 meaning nil, then elements
//   - pointers: 0 (nil) or 1 followed by the element
//   - structs: uvarint field count, then exported fields in declaration order
//     (fields tagged `waffle:"-"` are skipped). Data written by an older
//     version of a struct with fewer trailing fields still decodes; the new
//     fields are left at their zero value.
//   - any type whose pointer implements encoding.BinaryAppender and
//     encoding.BinaryUnmarshaler (time.Time, netip.Addr, ...) uses those
//     methods, length-prefixed.
//
// Interfaces, channels and funcs are rejected at compile time with an error.
func Compiled[T any]() (Codec[T], error) {
	op, err := compileValOp(reflect.TypeFor[T]())
	if err != nil {
		return nil, err
	}
	return compiledCodec[T]{op}, nil
}

type compiledCodec[T any] struct{ op *valOp }

func (c compiledCodec[T]) Append(dst []byte, v T) ([]byte, error) {
	return c.op.enc(dst, noescape(unsafe.Pointer(&v)))
}

func (c compiledCodec[T]) Decode(src []byte) (T, error) {
	var v T
	n, err := c.op.dec(src, noescape(unsafe.Pointer(&v)))
	if err == nil && n != len(src) {
		err = fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(src)-n)
	}
	return v, err
}

type valOp struct {
	enc func(dst []byte, p unsafe.Pointer) ([]byte, error)
	dec func(src []byte, p unsafe.Pointer) (int, error)
}

var valOpCache sync.Map // reflect.Type -> *valOp

var (
	binaryAppenderType    = reflect.TypeFor[encoding.BinaryAppender]()
	binaryUnmarshalerType = reflect.TypeFor[encoding.BinaryUnmarshaler]()
)

func compileValOp(t reflect.Type) (*valOp, error) {
	if op, ok := valOpCache.Load(t); ok {
		return op.(*valOp), nil
	}
	op, err := buildValOp(t, map[reflect.Type]*valOp{})
	if err != nil {
		return nil, err
	}
	actual, _ := valOpCache.LoadOrStore(t, op)
	return actual.(*valOp), nil
}

// encodedFields lists the struct fields that participate in encoding.
func encodedFields(t reflect.Type) []reflect.StructField {
	var out []reflect.StructField
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() || f.Tag.Get("waffle") == "-" {
			continue
		}
		out = append(out, f)
	}
	return out
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, args...)...)
}

func readUvarint(src []byte) (uint64, int, error) {
	v, n := binary.Uvarint(src)
	if n <= 0 {
		return 0, 0, corruptf("bad varint")
	}
	return v, n, nil
}

// readLen reads a length prefix and checks it against the remaining input so
// corrupt data cannot trigger huge allocations.
func readLen(src []byte, minElem int) (int, int, error) {
	v, n, err := readUvarint(src)
	if err != nil {
		return 0, 0, err
	}
	if minElem > 0 && v > uint64(len(src)-n)/uint64(minElem) {
		return 0, 0, corruptf("length %d exceeds input", v)
	}
	return int(v), n, nil
}

// readCount reads a (len+1, 0 = nil) collection header and checks that len
// elements of at least minElem bytes each can fit in the remaining input.
func readCount(src []byte, minElem int) (l int, isNil bool, n int, err error) {
	v, n, err := readUvarint(src)
	if err != nil {
		return 0, false, 0, err
	}
	if v == 0 {
		return 0, true, n, nil
	}
	if v-1 > uint64(len(src)-n)/uint64(minElem) {
		return 0, false, 0, corruptf("length %d exceeds input", v-1)
	}
	return int(v - 1), false, n, nil
}

func buildValOp(t reflect.Type, seen map[reflect.Type]*valOp) (*valOp, error) {
	if op, ok := seen[t]; ok {
		return op, nil // recursive type: closures call through op lazily
	}
	op := &valOp{}
	seen[t] = op

	pt := reflect.PointerTo(t)
	if pt.Implements(binaryAppenderType) && pt.Implements(binaryUnmarshalerType) {
		*op = binaryMarshalerOp(t)
		return op, nil
	}

	switch t.Kind() {
	case reflect.Bool:
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			if *(*bool)(p) {
				return append(dst, 1), nil
			}
			return append(dst, 0), nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			if len(src) < 1 || src[0] > 1 {
				return 0, corruptf("bad bool")
			}
			*(*bool)(p) = src[0] == 1
			return 1, nil
		}

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		size := t.Size()
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return binary.AppendUvarint(dst, loadUint(p, size)), nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			v, n, err := readUvarint(src)
			if err != nil {
				return 0, err
			}
			storeUint(p, size, v)
			return n, nil
		}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		size := t.Size()
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return binary.AppendVarint(dst, loadInt(p, size)), nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			v, n := binary.Varint(src)
			if n <= 0 {
				return 0, corruptf("bad varint")
			}
			storeUint(p, size, uint64(v))
			return n, nil
		}

	case reflect.Float32:
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return binary.LittleEndian.AppendUint32(dst, math.Float32bits(*(*float32)(p))), nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			if len(src) < 4 {
				return 0, corruptf("short float32")
			}
			*(*float32)(p) = math.Float32frombits(binary.LittleEndian.Uint32(src))
			return 4, nil
		}

	case reflect.Float64:
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			return binary.LittleEndian.AppendUint64(dst, math.Float64bits(*(*float64)(p))), nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			if len(src) < 8 {
				return 0, corruptf("short float64")
			}
			*(*float64)(p) = math.Float64frombits(binary.LittleEndian.Uint64(src))
			return 8, nil
		}

	case reflect.Complex64, reflect.Complex128:
		half := t.Size() / 2
		partT := reflect.TypeFor[float64]()
		if half == 4 {
			partT = reflect.TypeFor[float32]()
		}
		part, _ := buildValOp(partT, seen)
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			dst, _ = part.enc(dst, p)
			return part.enc(dst, unsafe.Add(p, half))
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			n1, err := part.dec(src, p)
			if err != nil {
				return 0, err
			}
			n2, err := part.dec(src[n1:], unsafe.Add(p, half))
			return n1 + n2, err
		}

	case reflect.String:
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			s := *(*string)(p)
			dst = binary.AppendUvarint(dst, uint64(len(s)))
			return append(dst, s...), nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			l, n, err := readLen(src, 1)
			if err != nil {
				return 0, err
			}
			*(*string)(p) = string(src[n : n+l])
			return n + l, nil
		}

	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
				b := *(*[]byte)(p)
				if b == nil {
					return append(dst, 0), nil
				}
				dst = binary.AppendUvarint(dst, uint64(len(b))+1)
				return append(dst, b...), nil
			}
			op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
				l, isNil, n, err := readCount(src, 1)
				if err != nil || isNil {
					return n, err
				}
				b := make([]byte, l)
				copy(b, src[n:])
				*(*[]byte)(p) = b
				return n + l, nil
			}
			break
		}
		elem, err := buildValOp(t.Elem(), seen)
		if err != nil {
			return nil, err
		}
		size := t.Elem().Size()
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			h := (*sliceHeader)(p)
			if h.data == nil {
				return append(dst, 0), nil
			}
			dst = binary.AppendUvarint(dst, uint64(h.len)+1)
			var err error
			for i := range h.len {
				if dst, err = elem.enc(dst, unsafe.Add(h.data, uintptr(i)*size)); err != nil {
					return dst, err
				}
			}
			return dst, nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			l, isNil, n, err := readCount(src, 1)
			if err != nil || isNil {
				return n, err
			}
			s := reflect.MakeSlice(t, l, l)
			data := s.UnsafePointer()
			pos := n
			for i := range l {
				m, err := elem.dec(src[pos:], unsafe.Add(data, uintptr(i)*size))
				if err != nil {
					return 0, err
				}
				pos += m
			}
			reflect.NewAt(t, p).Elem().Set(s)
			return pos, nil
		}

	case reflect.Array:
		elem, err := buildValOp(t.Elem(), seen)
		if err != nil {
			return nil, err
		}
		l, size := t.Len(), t.Elem().Size()
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			var err error
			for i := range l {
				if dst, err = elem.enc(dst, unsafe.Add(p, uintptr(i)*size)); err != nil {
					return dst, err
				}
			}
			return dst, nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			pos := 0
			for i := range l {
				m, err := elem.dec(src[pos:], unsafe.Add(p, uintptr(i)*size))
				if err != nil {
					return 0, err
				}
				pos += m
			}
			return pos, nil
		}

	case reflect.Pointer:
		elemT := t.Elem()
		elem, err := buildValOp(elemT, seen)
		if err != nil {
			return nil, err
		}
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			ptr := *(*unsafe.Pointer)(p)
			if ptr == nil {
				return append(dst, 0), nil
			}
			return elem.enc(append(dst, 1), ptr)
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			if len(src) < 1 || src[0] > 1 {
				return 0, corruptf("bad pointer tag")
			}
			if src[0] == 0 {
				return 1, nil
			}
			nv := reflect.New(elemT)
			m, err := elem.dec(src[1:], nv.UnsafePointer())
			if err != nil {
				return 0, err
			}
			reflect.NewAt(t, p).Elem().Set(nv)
			return 1 + m, nil
		}

	case reflect.Map:
		kt, vt := t.Key(), t.Elem()
		kop, err := buildValOp(kt, seen)
		if err != nil {
			return nil, err
		}
		vop, err := buildValOp(vt, seen)
		if err != nil {
			return nil, err
		}
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			m := reflect.NewAt(t, p).Elem()
			if m.IsNil() {
				return append(dst, 0), nil
			}
			dst = binary.AppendUvarint(dst, uint64(m.Len())+1)
			k, v := reflect.New(kt).Elem(), reflect.New(vt).Elem()
			it := m.MapRange()
			var err error
			for it.Next() {
				k.SetIterKey(it)
				v.SetIterValue(it)
				if dst, err = kop.enc(dst, k.Addr().UnsafePointer()); err != nil {
					return dst, err
				}
				if dst, err = vop.enc(dst, v.Addr().UnsafePointer()); err != nil {
					return dst, err
				}
			}
			return dst, nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			l, isNil, n, err := readCount(src, 2)
			if err != nil || isNil {
				return n, err
			}
			m := reflect.MakeMapWithSize(t, l)
			pos := n
			for range l {
				k, v := reflect.New(kt), reflect.New(vt)
				a, err := kop.dec(src[pos:], k.UnsafePointer())
				if err != nil {
					return 0, err
				}
				pos += a
				b, err := vop.dec(src[pos:], v.UnsafePointer())
				if err != nil {
					return 0, err
				}
				pos += b
				m.SetMapIndex(k.Elem(), v.Elem())
			}
			reflect.NewAt(t, p).Elem().Set(m)
			return pos, nil
		}

	case reflect.Struct:
		fields := encodedFields(t)
		ops := make([]*valOp, len(fields))
		offs := make([]uintptr, len(fields))
		for i, f := range fields {
			fop, err := buildValOp(f.Type, seen)
			if err != nil {
				return nil, fmt.Errorf("field %s.%s: %w", t, f.Name, err)
			}
			ops[i], offs[i] = fop, f.Offset
		}
		nf := uint64(len(fields))
		op.enc = func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			dst = binary.AppendUvarint(dst, nf)
			var err error
			for i, f := range ops {
				if dst, err = f.enc(dst, unsafe.Add(p, offs[i])); err != nil {
					return dst, err
				}
			}
			return dst, nil
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			c, pos, err := readUvarint(src)
			if err != nil {
				return 0, err
			}
			if c > nf {
				return 0, corruptf("%s: stored value has %d fields, type has %d (written by a newer schema?)", t, c, nf)
			}
			for i := range int(c) {
				m, err := ops[i].dec(src[pos:], unsafe.Add(p, offs[i]))
				if err != nil {
					return 0, err
				}
				pos += m
			}
			return pos, nil
		}

	default:
		delete(seen, t)
		return nil, fmt.Errorf("waffle: type %s (kind %s) cannot be encoded automatically; supply a Codec", t, t.Kind())
	}
	return op, nil
}

func binaryMarshalerOp(t reflect.Type) valOp {
	return valOp{
		enc: func(dst []byte, p unsafe.Pointer) ([]byte, error) {
			a := reflect.NewAt(t, p).Interface().(encoding.BinaryAppender)
			mark := len(dst)
			dst, err := a.AppendBinary(dst)
			if err != nil {
				return dst, err
			}
			// Prepend the length: append the varint, then rotate it into place.
			n := len(dst) - mark
			var hdr [binary.MaxVarintLen64]byte
			h := binary.PutUvarint(hdr[:], uint64(n))
			dst = append(dst, hdr[:h]...)
			copy(dst[mark+h:], dst[mark:mark+n])
			copy(dst[mark:], hdr[:h])
			return dst, nil
		},
		dec: func(src []byte, p unsafe.Pointer) (int, error) {
			l, n, err := readLen(src, 1)
			if err != nil {
				return 0, err
			}
			u := reflect.NewAt(t, p).Interface().(encoding.BinaryUnmarshaler)
			if err := u.UnmarshalBinary(src[n : n+l]); err != nil {
				return 0, err
			}
			return n + l, nil
		},
	}
}

type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

func loadInt(p unsafe.Pointer, size uintptr) int64 {
	switch size {
	case 1:
		return int64(*(*int8)(p))
	case 2:
		return int64(*(*int16)(p))
	case 4:
		return int64(*(*int32)(p))
	default:
		return *(*int64)(p)
	}
}

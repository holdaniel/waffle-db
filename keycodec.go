package waffle

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"
)

// KeyCodec encodes keys into an order-preserving byte form: for any two keys
// a and b, bytes.Compare(AppendKey(nil, a), AppendKey(nil, b)) must agree with
// the logical ordering of a and b. That property is what makes Range and
// Prefix scans on a Bucket meaningful.
//
// AppendKey appends to dst and returns the extended slice; when dst has enough
// capacity it must not allocate.
type KeyCodec[K any] interface {
	AppendKey(dst []byte, k K) []byte
	DecodeKey(src []byte) (K, error)
}

// prefixKeyCodec is implemented by compiled key codecs that can emit a partial
// key for prefix scans.
type prefixKeyCodec[K any] interface {
	appendPrefix(dst []byte, k K, fields int) ([]byte, error)
}

// ErrCorrupt is returned (wrapped) when stored bytes cannot be decoded.
var ErrCorrupt = errors.New("waffle: corrupt or incompatible encoding")

// keyOp is an encoder/decoder pair for one type, operating on raw memory.
// Ops are compiled once per type with reflection and then run with no
// reflection on the hot path.
type keyOp struct {
	enc func(dst []byte, p unsafe.Pointer) []byte
	dec func(src []byte, p unsafe.Pointer) (int, error)

	// Set for struct keys: per-field ops and offsets, used for prefix scans.
	fields  []*keyOp
	offsets []uintptr
	// Set for string-like keys: append without the terminator (prefix scan).
	encPartial func(dst []byte, p unsafe.Pointer) []byte
}

var keyOpCache sync.Map // reflect.Type -> *keyOp

func compileKeyOp(t reflect.Type) (*keyOp, error) {
	if op, ok := keyOpCache.Load(t); ok {
		return op.(*keyOp), nil
	}
	op, err := buildKeyOp(t, map[reflect.Type]bool{})
	if err != nil {
		return nil, err
	}
	actual, _ := keyOpCache.LoadOrStore(t, op)
	return actual.(*keyOp), nil
}

var timeType = reflect.TypeFor[time.Time]()

func buildKeyOp(t reflect.Type, visiting map[reflect.Type]bool) (*keyOp, error) {
	if t == timeType {
		return timeKeyOp(), nil
	}
	switch t.Kind() {
	case reflect.Bool:
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				if *(*bool)(p) {
					return append(dst, 1)
				}
				return append(dst, 0)
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				if len(src) < 1 || src[0] > 1 {
					return 0, ErrCorrupt
				}
				*(*bool)(p) = src[0] == 1
				return 1, nil
			},
		}, nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		size := t.Size()
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				return appendUintBE(dst, loadUint(p, size), size)
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				if uintptr(len(src)) < size {
					return 0, ErrCorrupt
				}
				storeUint(p, size, readUintBE(src, size))
				return int(size), nil
			},
		}, nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		// Two's complement with the sign bit flipped sorts correctly as unsigned.
		size := t.Size()
		sign := uint64(1) << (size*8 - 1)
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				return appendUintBE(dst, loadUint(p, size)^sign, size)
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				if uintptr(len(src)) < size {
					return 0, ErrCorrupt
				}
				storeUint(p, size, readUintBE(src, size)^sign)
				return int(size), nil
			},
		}, nil

	case reflect.Float32:
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				return binary.BigEndian.AppendUint32(dst, uint32(orderFloat(uint64(math.Float32bits(*(*float32)(p))), 32)))
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				if len(src) < 4 {
					return 0, ErrCorrupt
				}
				*(*float32)(p) = math.Float32frombits(uint32(unorderFloat(uint64(binary.BigEndian.Uint32(src)), 32)))
				return 4, nil
			},
		}, nil

	case reflect.Float64:
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				return binary.BigEndian.AppendUint64(dst, orderFloat(math.Float64bits(*(*float64)(p)), 64))
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				if len(src) < 8 {
					return 0, ErrCorrupt
				}
				*(*float64)(p) = math.Float64frombits(unorderFloat(binary.BigEndian.Uint64(src), 64))
				return 8, nil
			},
		}, nil

	case reflect.String:
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				return appendTerminated(dst, *(*string)(p))
			},
			encPartial: func(dst []byte, p unsafe.Pointer) []byte {
				return appendEscaped(dst, *(*string)(p))
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				b, n, err := decodeTerminated(src)
				if err != nil {
					return 0, err
				}
				*(*string)(p) = string(b)
				return n, nil
			},
		}, nil

	case reflect.Slice:
		if t.Elem().Kind() != reflect.Uint8 {
			return nil, fmt.Errorf("waffle: slice key type %s is not supported (only []byte-like slices)", t)
		}
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				b := *(*[]byte)(p)
				return appendTerminated(dst, unsafe.String(unsafe.SliceData(b), len(b)))
			},
			encPartial: func(dst []byte, p unsafe.Pointer) []byte {
				b := *(*[]byte)(p)
				return appendEscaped(dst, unsafe.String(unsafe.SliceData(b), len(b)))
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				b, n, err := decodeTerminated(src)
				if err != nil {
					return 0, err
				}
				*(*[]byte)(p) = bytes.Clone(b)
				return n, nil
			},
		}, nil

	case reflect.Array:
		elem, err := buildKeyOp(t.Elem(), visiting)
		if err != nil {
			return nil, err
		}
		n, size := t.Len(), t.Elem().Size()
		return &keyOp{
			enc: func(dst []byte, p unsafe.Pointer) []byte {
				for i := range n {
					dst = elem.enc(dst, unsafe.Add(p, uintptr(i)*size))
				}
				return dst
			},
			dec: func(src []byte, p unsafe.Pointer) (int, error) {
				pos := 0
				for i := range n {
					m, err := elem.dec(src[pos:], unsafe.Add(p, uintptr(i)*size))
					if err != nil {
						return 0, err
					}
					pos += m
				}
				return pos, nil
			},
		}, nil

	case reflect.Struct:
		if visiting[t] {
			return nil, fmt.Errorf("waffle: recursive key type %s is not supported", t)
		}
		visiting[t] = true
		defer delete(visiting, t)
		op := &keyOp{}
		for _, f := range encodedFields(t) {
			fop, err := buildKeyOp(f.Type, visiting)
			if err != nil {
				return nil, fmt.Errorf("field %s.%s: %w", t, f.Name, err)
			}
			op.fields = append(op.fields, fop)
			op.offsets = append(op.offsets, f.Offset)
		}
		fields, offsets := op.fields, op.offsets
		op.enc = func(dst []byte, p unsafe.Pointer) []byte {
			for i, f := range fields {
				dst = f.enc(dst, unsafe.Add(p, offsets[i]))
			}
			return dst
		}
		op.dec = func(src []byte, p unsafe.Pointer) (int, error) {
			pos := 0
			for i, f := range fields {
				m, err := f.dec(src[pos:], unsafe.Add(p, offsets[i]))
				if err != nil {
					return 0, err
				}
				pos += m
			}
			return pos, nil
		}
		return op, nil
	}
	return nil, fmt.Errorf("waffle: key type %s (kind %s) is not orderable; supply a KeyCodec", t, t.Kind())
}

func timeKeyOp() *keyOp {
	// 8 bytes of order-preserving signed seconds + 4 bytes of nanoseconds.
	// Covers the full time.Time range; decodes as UTC.
	const sign = uint64(1) << 63
	return &keyOp{
		enc: func(dst []byte, p unsafe.Pointer) []byte {
			t := (*time.Time)(p)
			dst = binary.BigEndian.AppendUint64(dst, uint64(t.Unix())^sign)
			return binary.BigEndian.AppendUint32(dst, uint32(t.Nanosecond()))
		},
		dec: func(src []byte, p unsafe.Pointer) (int, error) {
			if len(src) < 12 {
				return 0, ErrCorrupt
			}
			sec := int64(binary.BigEndian.Uint64(src) ^ sign)
			nsec := int64(binary.BigEndian.Uint32(src[8:]))
			if nsec >= 1e9 {
				return 0, ErrCorrupt
			}
			*(*time.Time)(p) = time.Unix(sec, nsec).UTC()
			return 12, nil
		},
	}
}

func loadUint(p unsafe.Pointer, size uintptr) uint64 {
	switch size {
	case 1:
		return uint64(*(*uint8)(p))
	case 2:
		return uint64(*(*uint16)(p))
	case 4:
		return uint64(*(*uint32)(p))
	default:
		return *(*uint64)(p)
	}
}

func storeUint(p unsafe.Pointer, size uintptr, v uint64) {
	switch size {
	case 1:
		*(*uint8)(p) = uint8(v)
	case 2:
		*(*uint16)(p) = uint16(v)
	case 4:
		*(*uint32)(p) = uint32(v)
	default:
		*(*uint64)(p) = v
	}
}

func appendUintBE(dst []byte, v uint64, size uintptr) []byte {
	switch size {
	case 1:
		return append(dst, byte(v))
	case 2:
		return binary.BigEndian.AppendUint16(dst, uint16(v))
	case 4:
		return binary.BigEndian.AppendUint32(dst, uint32(v))
	default:
		return binary.BigEndian.AppendUint64(dst, v)
	}
}

func readUintBE(src []byte, size uintptr) uint64 {
	switch size {
	case 1:
		return uint64(src[0])
	case 2:
		return uint64(binary.BigEndian.Uint16(src))
	case 4:
		return uint64(binary.BigEndian.Uint32(src))
	default:
		return binary.BigEndian.Uint64(src)
	}
}

// orderFloat maps IEEE-754 bits so that unsigned byte order matches numeric
// order: negatives get all bits flipped, positives get the sign bit set.
func orderFloat(bits uint64, width uint) uint64 {
	sign := uint64(1) << (width - 1)
	mask := uint64(1)<<width - 1
	if width == 64 {
		mask = math.MaxUint64
	}
	if bits&sign != 0 {
		return ^bits & mask
	}
	return bits | sign
}

func unorderFloat(bits uint64, width uint) uint64 {
	sign := uint64(1) << (width - 1)
	mask := uint64(1)<<width - 1
	if width == 64 {
		mask = math.MaxUint64
	}
	if bits&sign != 0 {
		return bits &^ sign
	}
	return ^bits & mask
}

// String-like key components are escaped (0x00 -> 0x00 0xFF) and terminated
// with 0x00 0x01. This keeps them self-delimiting inside composite keys while
// preserving lexicographic order: the terminator sorts before any escaped NUL
// and before any non-NUL byte, so "ab" < "ab\x00" < "abc".

func appendEscaped(dst []byte, s string) []byte {
	for {
		i := strings.IndexByte(s, 0)
		if i < 0 {
			return append(dst, s...)
		}
		dst = append(dst, s[:i]...)
		dst = append(dst, 0x00, 0xFF)
		s = s[i+1:]
	}
}

func appendTerminated(dst []byte, s string) []byte {
	return append(appendEscaped(dst, s), 0x00, 0x01)
}

// decodeTerminated returns the unescaped bytes and the number of input bytes
// consumed. The returned slice aliases src when no escapes were present.
func decodeTerminated(src []byte) ([]byte, int, error) {
	i := bytes.IndexByte(src, 0)
	if i < 0 || i+1 >= len(src) {
		return nil, 0, ErrCorrupt
	}
	if src[i+1] == 0x01 {
		return src[:i], i + 2, nil // fast path: no escapes
	}
	out := make([]byte, 0, len(src))
	pos := 0
	for {
		i := bytes.IndexByte(src[pos:], 0)
		if i < 0 || pos+i+1 >= len(src) {
			return nil, 0, ErrCorrupt
		}
		out = append(out, src[pos:pos+i]...)
		switch src[pos+i+1] {
		case 0x01:
			return out, pos + i + 2, nil
		case 0xFF:
			out = append(out, 0)
			pos += i + 2
		default:
			return nil, 0, ErrCorrupt
		}
	}
}

// compiledKeyCodec runs a compiled keyOp against a value of type K.
type compiledKeyCodec[K any] struct{ op *keyOp }

func (c compiledKeyCodec[K]) AppendKey(dst []byte, k K) []byte {
	return c.op.enc(dst, noescape(unsafe.Pointer(&k)))
}

func (c compiledKeyCodec[K]) DecodeKey(src []byte) (K, error) {
	var k K
	n, err := c.op.dec(src, noescape(unsafe.Pointer(&k)))
	if err == nil && n != len(src) {
		err = ErrCorrupt
	}
	return k, err
}

func (c compiledKeyCodec[K]) appendPrefix(dst []byte, k K, fields int) ([]byte, error) {
	p := noescape(unsafe.Pointer(&k))
	switch {
	case c.op.encPartial != nil:
		return c.op.encPartial(dst, p), nil
	case c.op.fields != nil:
		if fields < 0 || fields > len(c.op.fields) {
			return nil, fmt.Errorf("waffle: prefix of %d fields, key has %d", fields, len(c.op.fields))
		}
		for i := range fields {
			dst = c.op.fields[i].enc(dst, unsafe.Add(p, c.op.offsets[i]))
		}
		return dst, nil
	}
	return nil, errors.New("waffle: Prefix requires a struct, string or []byte key")
}

// OrderedKey returns the compiled, order-preserving KeyCodec for K. Supported
// key types: bool, all integer and float kinds, string and []byte kinds,
// time.Time, arrays of those, and structs composed of those (compared field
// by field in declaration order). Named types such as `type UserID uint64`
// work too.
func OrderedKey[K any]() (KeyCodec[K], error) {
	op, err := compileKeyOp(reflect.TypeFor[K]())
	if err != nil {
		return nil, err
	}
	return compiledKeyCodec[K]{op}, nil
}

// noescape hides a pointer from escape analysis so that encoding a value via
// an indirect call does not force it onto the heap. Safe here because the
// compiled ops never retain the pointer beyond the call. (Same trick the Go
// runtime and several std packages use.)
//
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return *(*unsafe.Pointer)(unsafe.Pointer(&x))
}

//go:build go1.18 || go1.19 || go1.20

package minswiss

import (
	"math/bits"
	"unsafe"
)

//goland:noinspection GoUnusedFunction
func main() {
	m := NewSwissMap[int64, int64](16)
	m.Put(1, 4)
	m.Put(12, 45)
	m.Put(123, 456)
	println(m.Get(123))
	println(m.Get(456))
}

// Get returns the |value| mapped by |key| if one exists.
func (m *SwissMap[K, V]) Get(key K) (value V, ok bool) {
	h := m.hash.Hash64(key)
	hi, lo := uint32((h&swissH1Mask)>>7), int8(h&swissH2Mask)
	mk := uint8(hi)
	size := uint32(len(m.groups))
	g := uint32((uint64(hi) * uint64(size)) >> 32)
	for { // inlined find loop
		meta := (*uint64)(unsafe.Pointer(&m.ctrl[g]))
		matches := swissMetaMatchH2(meta, lo)
		for matches != 0 {
			s := swissNextMatch(&matches)
			if m.ctrl[g].masks[s] == mk && key == m.groups[g].keys[s] {
				value, ok = m.groups[g].values[s], true
				return
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(meta)
		if matches != 0 {
			ok = false
			return
		}
		m.UnnecessaryCmp += 1
		g += 1 // linear probing
		if g >= size {
			g = 0
		}
	}
}

// Put attempts to insert |key| and |value|
func (m *SwissMap[K, V]) Put(key K, value V) {
	h := m.hash.Hash64(key)
	hi, lo := uint32((h&swissH1Mask)>>7), int8(h&swissH2Mask)
	mk := uint8(hi)
	size := uint32(len(m.groups))
	g := uint32((uint64(hi) * uint64(size)) >> 32)
	for { // inlined find loop
		meta := (*uint64)(unsafe.Pointer(&m.ctrl[g].flags))
		matches := swissMetaMatchH2(meta, lo)
		for matches != 0 {
			s := swissNextMatch(&matches)
			if m.ctrl[g].masks[s] == mk && key == m.groups[g].keys[s] { // update
				m.groups[g].keys[s] = key
				m.groups[g].values[s] = value
				return
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(meta)
		if matches != 0 { // insert
			s := swissNextMatch(&matches)
			m.groups[g].keys[s] = key
			m.groups[g].values[s] = value
			m.ctrl[g].flags[s] = lo
			m.ctrl[g].masks[s] = mk
			m.resident++
			return
		}
		g += 1 // linear probing
		if g >= size {
			g = 0
		}
	}
}

// Count returns the number of elements in the SwissMap.
func (m *SwissMap[K, V]) Count() int {
	return int(m.resident - m.dead)
}

func (m *SwissMap[K, V]) GetResident() uint32 {
	return m.resident
}

// NewSwissMap constructs a SwissMap.
//
//goland:noinspection GoUnusedExportedFunction
func NewSwissMap[K comparable, V any](sz uint32) (m *SwissMap[K, V]) {
	groups := (sz + swissMaxAvgGroupLoad - 1) / swissMaxAvgGroupLoad
	if groups == 0 {
		groups = 1
	} else if groups > 128 {
		groups += groups >> 1
	}

	m = &SwissMap[K, V]{
		ctrl:   make([]swissMetadata, groups),
		groups: make([]swissGroup[K, V], groups),
		hash:   NewHasher[K](),
		limit:  groups * swissMaxAvgGroupLoad,
	}

	t64 := *(*[]swissMeta128)(unsafe.Pointer(&m.ctrl))
	for i := range t64 {
		t64[i].flag = swissEmpty64
	}
	return
}

// SwissMap is an open-addressing hash map
// based on Abseil's flat_hash_map.
type SwissMap[K comparable, V any] struct {
	ctrl           []swissMetadata
	groups         []swissGroup[K, V]
	hash           Hasher[K]
	UnnecessaryCmp int
	resident       uint32
	dead           uint32
	limit          uint32
}

// swissMetadata is the swissH2 swissMetadata array for a swissGroup.
// find operations first probe the controls bytes
// to filter candidates before matching keys
type swissMetadata struct {
	flags [swissGroupSize]int8
	masks [swissGroupSize]uint8
}

type swissMeta128 struct {
	flag uint64
	mask uint64
}

// swissGroup is a swissGroup of 16 key-value pairs
type swissGroup[K comparable, V any] struct {
	keys   [swissGroupSize]K
	values [swissGroupSize]V
}

// noescape hides a pointer from escape analysis. It is the identity function
// but escape analysis doesn't think the output depends on the input.
// noescape is inlined and currently compiles down to zero instructions.
// USE CAREFULLY!
// This was copied from the runtime (via pkg "strings"); see issues 23382 and 7921.
//
//go:nosplit
//go:nocheckptr
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	//goland:noinspection GoVetUnsafePointer
	return unsafe.Pointer(x ^ 0)
}

type mapiface struct {
	typ *maptype
	val *hmap
}

// go/src/runtime/type.go
type maptype struct {
	typ    _type
	key    *_type
	elem   *_type
	bucket *_type
	// function for hashing keys (ptr to key, seed) -> hash
	hasher     func(unsafe.Pointer, uintptr) uintptr
	keysize    uint8
	elemsize   uint8
	bucketsize uint16
	flags      uint32
}

// go/src/runtime/map.go
type hmap struct {
	count     int
	flags     uint8
	B         uint8
	noverflow uint16
	// hash seed
	hash0      uint32
	buckets    unsafe.Pointer
	oldbuckets unsafe.Pointer
	nevacuate  uintptr
	// true type is *mapextra,
	// but we don't need this data
	extra unsafe.Pointer
}

// go/src/runtime/type.go
type tflag uint8
type nameOff int32
type typeOff int32

// go/src/runtime/type.go
type _type struct {
	size       uintptr
	ptrdata    uintptr
	hash       uint32
	tflag      tflag
	align      uint8
	fieldAlign uint8
	kind       uint8
	equal      func(unsafe.Pointer, unsafe.Pointer) bool
	gcdata     *byte
	str        nameOff
	ptrToThis  typeOff
}

const (
	swissGroupSize       = 8
	swissMaxAvgGroupLoad = 7

	swissLoBits uint64 = 0x0101010101010101
	swissHiBits uint64 = 0x8080808080808080
)

type hashfn func(unsafe.Pointer, uintptr) uintptr

// Hasher hashes values of type K.
// Uses runtime AES-based hashing.
type Hasher[K comparable] struct {
	hash hashfn
	seed uintptr
}

// NewHasher creates a new Hasher[K] with a random seed.
func NewHasher[K comparable]() Hasher[K] {
	h, ss := getRuntimeHasher[K]()
	return Hasher[K]{hash: h, seed: ss}
}

// Hash64 hashes |key|.
func (h Hasher[K]) Hash64(key K) uint64 {
	// promise to the compiler that pointer
	// |p| does not escape the stack.
	p := noescape(unsafe.Pointer(&key))
	return uint64(h.hash(p, h.seed))
}

func getRuntimeHasher[K comparable]() (h hashfn, seed uintptr) {
	a := any(make(map[K]struct{}))
	i := (*mapiface)(unsafe.Pointer(&a))
	h, seed = i.typ.hasher, uintptr(i.val.hash0)
	return
}

func swissMetaMatchH2(m *uint64, h int8) uint64 {
	// https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	x := *m ^ (swissLoBits * uint64(h))
	return ((x - swissLoBits) & ^(x)) & swissHiBits
}

func swissMetaMatchEmpty(m *uint64) uint64 {
	x := *m ^ swissHiBits
	return ((x - swissLoBits) & ^(x)) & swissHiBits
}

func swissNextMatch(b *uint64) uint32 {
	s := uint32(bits.TrailingZeros64(*b))
	*b &= ^(1 << s) // clear bit |s|
	return s >> 3   // div by 8
}

//goland:noinspection GoUnusedConst
const (
	swissMaxLoadFactor = float32(swissMaxAvgGroupLoad) / float32(swissGroupSize)
)

const (
	swissH1Mask  uint64 = 0xffff_ffff_ffff_ff80
	swissH2Mask  uint64 = 0x0000_0000_0000_007f
	swissEmpty64 uint64 = 0x8080_8080_8080_8080 // 0b1000_0000
)

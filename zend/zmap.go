//go:build go1.18 || go1.19 || go1.20

package zend

import (
	"math/bits"
	"unsafe"
)

//goland:noinspection GoUnusedFunction
func main() {
	m := NewZMap(16)
	m.Put(TZvalLong(1), TZvalLong(4))
	m.Put(TZvalLong(12), TZvalLong(45))
	m.Put(TZvalLong(123), TZvalLong(456))
	println(m.Get(TZvalLong(123)))
	println(m.Get(TZvalLong(456)))

	println(m.Get(TZvalStr("123")))
}

// Get returns the |value| mapped by |key| if one exists.
func (m *ZMap) Get(key *TZval) (*TZval, bool) {
	keyFixed, hh := ZMapTZvalHash(key)
	hi, lo := uint32((hh&swissH1Mask)>>7), int8(hh&swissH2Mask)
	size := uint32(len(m.groups))
	idx, _, _ := m.ZMapFind(keyFixed, hi, size, lo)
	if idx >= 0 {
		return m.entry[idx].value, true
	}
	//goland:noinspection GoVetUnsafePointer
	return (*TZval)(unsafe.Pointer(_TZvalUndef)), false
}

// Put attempts to insert |key| and |value|
func (m *ZMap) Put(key *TZval, value *TZval) {
	keyFixed, hh := ZMapTZvalHash(key)
	hi, lo := uint32((hh&swissH1Mask)>>7), int8(hh&swissH2Mask)
	size := uint32(len(m.groups))
	idx, g, s := m.ZMapFind(keyFixed, hi, size, lo)
	if idx >= 0 {
		m.entry[idx].key = key
		m.entry[idx].value = value
		return
	}

	m.entry = append(m.entry, ZEntry{key: keyFixed, value: value})
	m.groups[g][s] = uint32(len(m.entry) - 1)
	m.ctrl[g].flags[s] = lo
	m.ctrl[g].masks[s] = uint8(hi)
	m.resident++
	return
}

//go:nosplit
func (m *ZMap) ZMapFind(zp1 *TZval, hi uint32, size uint32, lo int8) (int, uint32, uint32) {
	g := uint32((uint64(hi) * uint64(size)) >> 32)
	for {
		// meta := (*uint64)(unsafe.Pointer(&m.ctrl[g].flags))
		//goland:noinspection GoVetUnsafePointer
		meta := (*uint64)(unsafe.Pointer((*ZMapStruct)(unsafe.Pointer(m)).ctrlPtr + uintptr(g<<4)))
		matches := swissMetaMatchH2(meta, lo)
		for matches != 0 {
			s := swissNextMatch(&matches)
			if *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(meta)) + uintptr(s+8))) == uint8(hi) {
				// idx := m.groups[g][s]
				//goland:noinspection GoVetUnsafePointer
				idx := int(*(*uint32)(unsafe.Pointer((*ZMapStruct)(unsafe.Pointer(m)).groupsPtr + uintptr(g<<5+s<<2))))
				// zp2 := m.entry[idx].key
				//goland:noinspection GoVetUnsafePointer
				zp2 := *(**TZval)(unsafe.Pointer((*ZMapStruct)(unsafe.Pointer(m)).entryPtr + uintptr(idx<<4)))
				// if ZMapTZvalEqual(zp1, zp2) { return idx, g, s }

				//region ZMapTZvalEqual inline
				zMapTZvalEqual := false
				for {
					if uintptr(unsafe.Pointer(zp1)) == uintptr(unsafe.Pointer(zp2)) {
						zMapTZvalEqual = true
						break
					}

					tpy1 := (*ZvalFlags)(unsafe.Pointer(&zp1)).Typ
					tpy2 := (*ZvalFlags)(unsafe.Pointer(&zp2)).Typ

					// if one is tagged, the other almost is tagged, just test uintptr(zp1) == uintptr(zp2)
					if tpy1 > 127 || tpy2 > 127 {
						zMapTZvalEqual = false
						break
					}

					// test Zval for U_LONG or U_DOUBLE
					tpy1 = (*_Zval)(unsafe.Pointer(zp1)).Typ
					tpy2 = (*_Zval)(unsafe.Pointer(zp2)).Typ

					// Zval type, test Typ and Val field as uintptr
					if tpy1 > 0 || tpy2 > 0 { // U_LONG or U_DOUBLE
						zMapTZvalEqual = tpy1 == tpy2 &&
							(*_Zval)(unsafe.Pointer(zp1)).Val == (*_Zval)(unsafe.Pointer(zp2)).Val
						break
					}

					// test PZval must have ptr and cannot been tagged
					tpy1 = zp1.Typ
					tpy2 = zp2.Typ

					// long str and both has hash(str) in H field
					if tpy1 == IS_STRING && tpy2 == IS_STRING {
						if (*_ZString)(zp1.Ptr).Len != (*_ZString)(zp2.Ptr).Len {
							zMapTZvalEqual = false
							break
						}
						if (*_ZString)(zp1.Ptr).H != (*_ZString)(zp2.Ptr).H {
							zMapTZvalEqual = false
							break
						}
						zMapTZvalEqual = *(*string)(zp1.Ptr) == *(*string)(zp2.Ptr)
						break
					}

					// other PZval type, test Typ and Ptr field
					zMapTZvalEqual = tpy1 == tpy2 && uintptr(zp1.Ptr) == uintptr(zp2.Ptr)
					break
				}
				//endregion

				if zMapTZvalEqual {
					return idx, g, s
				}
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(meta)
		if matches != 0 {
			s := swissNextMatch(&matches)
			return -1, g, s
		}
		g += 1 // linear probing
		if g >= size {
			g = 0
		}
	}
}

// Count returns the number of elements in the ZMap.
func (m *ZMap) Count() int {
	return int(m.resident - m.dead)
}

func (m *ZMap) GetResident() uint32 {
	return m.resident
}

//region ZMap help func

func Strs2TZvals(strs []string) []*TZval {
	zps := make([]*TZval, 0, len(strs))
	for _, s := range strs {
		zps = append(zps, TZvalStr(s))
	}
	return zps
}

func Ints2TZvals(ints []int64) []*TZval {
	zps := make([]*TZval, 0, len(ints))
	for _, l := range ints {
		zps = append(zps, TZvalLong(l))
	}
	return zps
}

type ZvalFlags struct {
	U2    uint32
	Extra uint16
	Flag  uint8
	Typ   uint8
}

type TZval struct {
	Ptr unsafe.Pointer
	ZvalFlags
}

type PZval struct {
	Ptr unsafe.Pointer
	ZvalFlags
}

type Zval struct {
	TZval
}

type LZval struct {
	ZvalFlags
	Lval int64
}

type DZval struct {
	ZvalFlags
	Dval float64
}

type _Zval struct {
	ZvalFlags
	Val uintptr
}

type _TLong struct {
	U2 int32
	S2 int16
	C2 int8
	C1 int8
}

type _SLong struct {
	U2 int32
	S2 int16
	S1 int16
}

type ZString struct {
	Bytes []byte
	H     uintptr
}

type _ZString struct {
	sliceStruct
	H uintptr
}

type sliceStruct struct {
	Ptr unsafe.Pointer
	Len int
	Cap int
}

func TZvalLong(l int64) *TZval {
	if l&Int56Test == 0 || l&Int56Test == Int56Test {
		(*ZvalFlags)(unsafe.Pointer(&l)).Typ = T_LONG
		//goland:noinspection GoVetUnsafePointer
		return (*TZval)(unsafe.Pointer(uintptr(l)))
	}
	zp := new(LZval)
	zp.Typ = U_LONG
	zp.Lval = l
	return (*TZval)(unsafe.Pointer(zp))
}

func TZvalStr(s string) *TZval {
	b := []byte(s)
	return TZvalBytes(b)
}

func TZvalBytes(b []byte) *TZval {
	l := len(b)
	if l <= 7 {
		ptr := uintptr(l)<<60 | _TZvalEmptyString
		i := *(*uintptr)((*sliceStruct)(unsafe.Pointer(&b)).Ptr)
		//goland:noinspection GoVetUnsafePointer
		return (*TZval)(unsafe.Pointer(i&TStringMasks[l] | ptr))
	}
	zp := new(TZval)
	zp.Ptr = unsafe.Pointer(&ZString{Bytes: b})
	zp.Typ = IS_STRING
	return zp
}

func (zp *TZval) TZLong_() int64 {
	tpy := (*ZvalFlags)(unsafe.Pointer(&zp)).Typ
	if tpy == T_LONG {
		l := int64(uintptr(unsafe.Pointer(zp)))
		(*_SLong)(unsafe.Pointer(&l)).S1 = int16((*_TLong)(unsafe.Pointer(&l)).C2)
		return l
	}
	return (*LZval)(unsafe.Pointer(zp)).Lval
}

var (
	PtrHashFunc func(uintptr) uintptr
	StrHashFunc func(string) uintptr

	//goland:noinspection GoSnakeCaseUsage
	TStringMasks = [8]uintptr{
		0x0000_0000_0000_0000,
		0x0000_0000_0000_00ff,
		0x0000_0000_0000_ffff,
		0x0000_0000_00ff_ffff,
		0x0000_0000_ffff_ffff,
		0x0000_00ff_ffff_ffff,
		0x0000_ffff_ffff_ffff,
		0x00ff_ffff_ffff_ffff,
	}
)

//goland:noinspection GoSnakeCaseUsage,GoUnusedConst
const (
	/* regular data types */

	IS_UNDEF uint8 = 0b0000_0000 // tagged 0b0000

	IS_ARRAY  uint8 = 0b0000_0001 // 0b0001
	IS_NULL   uint8 = 0b0000_0010 // tagged 0b0010
	IS_OBJECT uint8 = 0b0000_0011 // 0b0011
	IS_LONG   uint8 = 0b0000_0100 // tagged 0b0100

	// IS_CONSTANT_AST /* constant expressions */
	IS_CONSTANT_AST uint8 = 0b0000_0101 // 0b0101

	IS_STRING    uint8 = 0b0000_0110 // tagged 0b0110
	IS_REFERENCE uint8 = 0b0000_0111 // 0b0111
	IS_FALSE     uint8 = 0b0000_1000 // tagged 0b1000
	IS_RESOURCE  uint8 = 0b0000_1001 // 0b1001
	IS_TRUE      uint8 = 0b0000_1010 // tagged 0b1010

	IS_PTR uint8 = 0b0000_1011 // 0b1011
	/* internal types */

	IS_DOUBLE uint8 = 0b0000_1100 // tagged 0b1100

	IS_INDIRECT uint8 = 0b0000_1101 // 0b1101

	_IS_ERROR uint8 = 0b0000_1110 // tagged 0b1110

	IS_ALIAS_PTR uint8 = 0b0000_1111 // 0b1111

	/* fake types used only for type hinting (Z_TYPE(zv) can not use them) */

	_IS_BOOL    uint8 = 0b0001_0000 // 0b0001_0000
	IS_CALLABLE uint8 = 0b0001_0001 // 0b0001_0001
	IS_ITERABLE uint8 = 0b0001_0010 // 0b0001_0010
	IS_VOID     uint8 = 0b0001_0011 // 0b0001_0011
	_IS_NUMBER  uint8 = 0b0001_0100 // 0b0001_0100
)

//goland:noinspection GoSnakeCaseUsage,GoUnusedConst
const (
	/* Tagged Pointer *Zval */

	T_TMASK uint8 = 0b0000_1110
	T_UMASK uint8 = 0b1000_1110
	T_PMASK uint8 = 0b0000_0001

	T_LMASK uint8 = 0b0111_0000 // for T_STRING Len 0-7

	T_MASK uint8 = 0b1000_0000
	U_MASK uint8 = 0b0000_0000

	T_UNDEF  = IS_UNDEF | T_MASK
	T_NULL   = IS_NULL | T_MASK
	T_FALSE  = IS_FALSE | T_MASK
	T_TRUE   = IS_TRUE | T_MASK
	T_ERROR  = _IS_ERROR | T_MASK
	T_STRING = IS_STRING | T_MASK
	T_LONG   = IS_LONG | T_MASK

	/* Union Ptr in Zval */

	U_LONG   = IS_LONG | U_MASK
	U_DOUBLE = IS_DOUBLE | U_MASK

	T_PACKET_MASK   uintptr = 0x0100_0000_0000_0000
	T_UNPACKET_MASK uintptr = 0xfeff_ffff_ffff_ffff

	Z_LONG_MAX uintptr = 9223372036854775807

	T_STRING_MASK uintptr = 0x00ff_ffff_ffff_ffff
	// T_CLOBBERDEADPTR uint8 = 0b1101_1110 //  deaddeaddeade000
)

/* short integer max and min */

//goland:noinspection GoSnakeCaseUsage,GoUnusedConst
const (
	Int56Max  int64 = 36028797018963967  // 0 => 0x007f_ffff_ffff_ffff
	Int56Min  int64 = -36028797018963968 // -1 (0xffff_ffff_ffff_ffff) => 0xff80_0000_0000_0000
	Int56Test int64 = -36028797018963968 // 0xff80_0000_0000_0000
)

//goland:noinspection GoSnakeCaseUsage,GoUnusedConst
const (
	_TZvalUndef       = uintptr(T_UNDEF) << 56
	_TZvalNull        = uintptr(T_NULL) << 56
	_TZvalFalse       = uintptr(T_FALSE) << 56
	_TZvalTrue        = uintptr(T_TRUE) << 56
	_TZvalEmptyString = uintptr(T_STRING) << 56

	_TZvalZero = uintptr(T_LONG) << 56
	_TZvalOne  = uintptr(T_LONG)<<56 | 1

	_TZvalError = uintptr(T_ERROR) << 56
)

func init() {
	strHasher := NewHasher[string]()
	StrHashFunc = strHasher.Hash

	ptrHasher := NewHasher[uintptr]()
	PtrHashFunc = ptrHasher.Hash
}

// ZMapTZvalEqual test *TZval Equal, fast path, it is as well to inline
//
//goland:noinspection GoUnusedExportedFunction
func ZMapTZvalEqual(zp1 *TZval, zp2 *TZval) bool {
	if uintptr(unsafe.Pointer(zp1)) == uintptr(unsafe.Pointer(zp2)) {
		return true
	}

	tpy1 := (*ZvalFlags)(unsafe.Pointer(&zp1)).Typ
	tpy2 := (*ZvalFlags)(unsafe.Pointer(&zp2)).Typ

	// if one is tagged, the other almost is tagged, just test uintptr(zp1) == uintptr(zp2)
	if tpy1 > 127 || tpy2 > 127 {
		return uintptr(unsafe.Pointer(zp1)) == uintptr(unsafe.Pointer(zp2))
	}

	// test Zval for U_LONG or U_DOUBLE
	tpy1 = (*_Zval)(unsafe.Pointer(zp1)).Typ
	tpy2 = (*_Zval)(unsafe.Pointer(zp2)).Typ

	// Zval type, test Typ and Val field as uintptr
	if tpy1 > 0 || tpy2 > 0 { // U_LONG or U_DOUBLE
		return tpy1 == tpy2 && (*_Zval)(unsafe.Pointer(zp1)).Val == (*_Zval)(unsafe.Pointer(zp2)).Val
	}

	// test PZval must have ptr and cannot been tagged
	tpy1 = zp1.Typ
	tpy2 = zp2.Typ

	// long str and both has hash(str) in H field
	if tpy1 == IS_STRING && tpy2 == IS_STRING {
		if (*_ZString)(zp1.Ptr).Len != (*_ZString)(zp2.Ptr).Len {
			return false
		}
		if (*_ZString)(zp1.Ptr).H != (*_ZString)(zp2.Ptr).H {
			return false
		}
		return *(*string)(zp1.Ptr) == *(*string)(zp2.Ptr)
	}

	// other PZval type, test Typ and Ptr field
	return tpy1 == tpy2 && uintptr(zp1.Ptr) == uintptr(zp2.Ptr)
}

// ZMapTZvalHash hash(*TZval) and try to be tagged, it is as well to inline
func ZMapTZvalHash(zp *TZval) (*TZval, uintptr) {
	// test TZval tagged, use tagged and hash(tagged)
	tpy := (*ZvalFlags)(unsafe.Pointer(&zp)).Typ
	if tpy > 127 {
		return zp, PtrHashFunc(uintptr(unsafe.Pointer(zp)))
	}

	// test Zval for U_LONG or U_DOUBLE
	tpy = (*_Zval)(unsafe.Pointer(zp)).Typ
	if tpy > 0 { // U_LONG or U_DOUBLE
		// try tagged U_LONG val
		if tpy == IS_LONG {
			l := (*LZval)(unsafe.Pointer(zp)).Lval
			// short U_LONG use tagged and hash(tagged)
			if l&Int56Test == 0 || l&Int56Test == Int56Test {
				(*ZvalFlags)(unsafe.Pointer(&l)).Typ = T_LONG
				//goland:noinspection GoVetUnsafePointer
				tagged := (*TZval)(unsafe.Pointer(uintptr(l)))
				return tagged, PtrHashFunc(uintptr(unsafe.Pointer(tagged)))
			}
		}
		// U_LONG or U_DOUBLE as uintptr val, use ptr and hash(val)
		return zp, PtrHashFunc((*_Zval)(unsafe.Pointer(zp)).Val)
	}

	// test PZval ptr val
	tpy = zp.Typ

	if tpy == IS_STRING {
		l := (*_ZString)(zp.Ptr).Len
		h := (*_ZString)(zp.Ptr).H

		// l <= 7 short str use tagged and hash(tagged)
		if l <= 7 {
			if h != 0 {
				//goland:noinspection GoVetUnsafePointer
				return (*TZval)(unsafe.Pointer(h)), PtrHashFunc(h)
			}
			ptr := uintptr(l)<<60 | _TZvalEmptyString
			i := *(*uintptr)((*_ZString)(zp.Ptr).Ptr)
			//goland:noinspection GoVetUnsafePointer
			tagged := (*TZval)(unsafe.Pointer(i&TStringMasks[l] | ptr))
			// l <= 7 short str save tagged ptr to H
			(*_ZString)(zp.Ptr).H = uintptr(unsafe.Pointer(tagged))
			return tagged, PtrHashFunc(uintptr(unsafe.Pointer(tagged)))
		}

		// long str use ptr and hash(str)
		if h != 0 {
			return zp, h
		}
		h = StrHashFunc(*(*string)(zp.Ptr))
		// long str save hash to H
		(*_ZString)(zp.Ptr).H = h
		return zp, h
	}

	// tagged PZval has no ptr, so use tagged and hash(tagged)
	if tpy == IS_UNDEF || tpy == IS_NULL || tpy == IS_FALSE || tpy == IS_TRUE || tpy == _IS_ERROR {
		//goland:noinspection GoVetUnsafePointer
		tagged := (*TZval)(unsafe.Pointer(uintptr(tpy|T_MASK) << 56))
		return tagged, PtrHashFunc(uintptr(unsafe.Pointer(tagged)))
	}

	// other PZval has ptr, use ptr and hash(ptr)
	return zp, PtrHashFunc(uintptr(zp.Ptr))
}

//endregion

// swissZGroup is a swissZGroup of 8 key-value pairs
type swissZGroup [swissGroupSize]uint32

// ZMap is an open-addressing hash map
// based on Abseil's flat_hash_map.
type ZMap struct {
	ctrl         []swissMetadata
	groups       []swissZGroup
	entry        []ZEntry
	resident     uint32
	dead         uint32
	limit        uint32
	appendOffset uint32
}

type ZMapStruct struct {
	ctrlPtr uintptr
	ctrlLen int
	ctrlCap int

	groupsPtr uintptr
	groupsLen int
	groupsCap int

	entryPtr uintptr
	entryLen int
	entryCap int

	resident     uint32
	dead         uint32
	limit        uint32
	appendOffset uint32
}

type ZEntry struct {
	key   *TZval
	value *TZval
}

// NewZMap constructs a ZMap.
//
//goland:noinspection GoUnusedExportedFunction
func NewZMap(sz uint32) (m *ZMap) {
	groups := swissNumGroups(sz)
	m = &ZMap{
		ctrl:   make([]swissMetadata, groups),
		groups: make([]swissZGroup, groups), // swissGroupSize 8
		entry:  make([]ZEntry, 0, swissGroupSize),
		limit:  groups * swissMaxAvgGroupLoad,
	}
	t64 := *(*[]swissMeta128)(unsafe.Pointer(&m.ctrl))
	for i := range t64 {
		t64[i].flag = swissEmpty64
	}
	return
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

// swissNumGroups returns the minimum number of groups needed to store |n| elems.
func swissNumGroups(n uint32) (groups uint32) {
	groups = (n + swissMaxAvgGroupLoad - 1) / swissMaxAvgGroupLoad
	if groups == 0 {
		groups = 1
	} else if groups >= 128 {
		groups += groups >> 2
	}
	return
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

// Hash hashes |key|.
func (h Hasher[K]) Hash(key K) uintptr {
	// promise to the compiler that pointer
	// |p| does not escape the stack.
	p := noescape(unsafe.Pointer(&key))
	return h.hash(p, h.seed)
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
	swissH1Mask  uintptr = 0xffff_ffff_ffff_ff80
	swissH2Mask  uintptr = 0x0000_0000_0000_007f
	swissEmpty64 uint64  = 0x8080_8080_8080_8080 // 0b1000_0000
)

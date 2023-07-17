package zend

import (
	"math/bits"
	"unsafe"
)

const (
	swissGroupSize       = 8
	swissMaxAvgGroupLoad = 7

	swissLoBits uint64 = 0x0101010101010101
	swissHiBits uint64 = 0x8080808080808080
)

//goland:noinspection GoUnusedConst
const (
	swissMaxLoadFactor = float32(swissMaxAvgGroupLoad) / float32(swissGroupSize)
)

const (
	swissH1Mask    uint64 = 0xffff_ffff_ffff_ff80
	swissH2Mask    uint64 = 0x0000_0000_0000_007f
	swissEmpty     int8   = -128 // 0b1000_0000
	swissEmpty64   uint64 = 0x8080_8080_8080_8080
	swissTombstone int8   = -2 // 0b1111_1110

	splitSubMapLimit uint32  = 32 * 1024
	splitSubMapSize  uint32  = 256 // 2^8 uint8(swissH1) suffix 8bit
	flagSmallMap     uintptr = 0
	flagLargeMap     uintptr = 0x0000_0000_0000_0000_0001
)

// SwissMap is an open-addressing hash map
// based on Abseil's flat_hash_map.
type SwissMap[K comparable, V any] struct {
	flags uintptr
	hash  Hasher[K]
	SwissSubMap[K, V]
}

type SwissLargeMap[K comparable, V any] struct {
	flags uintptr
	hash  Hasher[K]
	subs  [splitSubMapSize]SwissSubMap[K, V]
}

type SwissSubMap[K comparable, V any] struct {
	ctrl     []swissMetadata
	groups   []swissGroup[K, V]
	resident uint32
	dead     uint32
	limit    uint32
	unused   uint32
}

// swissMetadata is the swissH2 swissMetadata array for a swissGroup.
// find operations first probe the controls bytes
// to filter candidates before matching keys
type swissMetadata [swissGroupSize]int8

// swissGroup is a swissGroup of 16 key-value pairs
type swissGroup[K comparable, V any] struct {
	keys   [swissGroupSize]K
	values [swissGroupSize]V
}

// swissH1 is a 57 bit hash prefix
type swissH1 uint64

// swissH2 is a 7 bit hash suffix
type swissH2 int8

// NewSwissMap constructs a SwissMap.
//
//goland:noinspection GoUnusedExportedFunction
func NewSwissMap[K comparable, V any](sz uint32) *SwissMap[K, V] {
	var (
		m *SwissMap[K, V]
	)
	groups := swissNumGroups(sz)
	if groups*swissGroupSize > splitSubMapLimit {
		lm := &SwissLargeMap[K, V]{
			flags: flagLargeMap,
			hash:  NewHasher[K](),
		}
		groups = groups / splitSubMapSize
		for sdx := uint32(0); sdx < splitSubMapSize; sdx++ {
			lm.subs[sdx].ctrl = make([]swissMetadata, groups)
			lm.subs[sdx].groups = make([]swissGroup[K, V], groups)
			lm.subs[sdx].limit = groups * swissMaxAvgGroupLoad

			t64 := (*[]uint64)(unsafe.Pointer(&lm.subs[sdx].ctrl))
			for i := range *t64 {
				(*t64)[i] = swissEmpty64
			}
		}
		m = (*SwissMap[K, V])(unsafe.Pointer(lm))
	} else {
		m = &SwissMap[K, V]{
			flags: flagSmallMap,
			hash:  NewHasher[K](),
			SwissSubMap: SwissSubMap[K, V]{
				ctrl:   make([]swissMetadata, groups),
				groups: make([]swissGroup[K, V], groups),
				limit:  groups * swissMaxAvgGroupLoad,
			},
		}

		t64 := (*[]uint64)(unsafe.Pointer(&m.ctrl))
		for i := range *t64 {
			(*t64)[i] = swissEmpty64
		}
	}
	return m
}

// Has returns true if |key| is present in |m|.
func (m *SwissMap[K, V]) Has(key K) (ok bool) {
	hi, lo := swissSplitHash(m.hash.Hash64(key))
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		sdx := uint8(hi)

		g := swissProbeStart(hi, len(lm.subs[sdx].groups))
		for { // inlined find loop
			matches := swissMetaMatchH2(&lm.subs[sdx].ctrl[g], lo)
			for matches != 0 {
				s := swissNextMatch(&matches)
				if key == lm.subs[sdx].groups[g].keys[s] {
					ok = true
					return
				}
			}
			// |key| is not in swissGroup |g|,
			// stop probing if we see an swissEmpty slot
			matches = swissMetaMatchEmpty(&lm.subs[sdx].ctrl[g])
			if matches != 0 {
				ok = false
				return
			}
			g += 1 // linear probing
			if g >= uint32(len(lm.subs[sdx].groups)) {
				g = 0
			}
		}
		//goland:noinspection GoUnreachableCode
		return
	}

	g := swissProbeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := swissMetaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := swissNextMatch(&matches)
			if key == m.groups[g].keys[s] {
				ok = true
				return
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Get returns the |value| mapped by |key| if one exists.
func (m *SwissMap[K, V]) Get(key K) (value V, ok bool) {
	hi, lo := swissSplitHash(m.hash.Hash64(key))
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		sdx := uint8(hi)

		g := swissProbeStart(hi, len(lm.subs[sdx].groups))
		for { // inlined find loop
			matches := swissMetaMatchH2(&lm.subs[sdx].ctrl[g], lo)
			for matches != 0 {
				s := swissNextMatch(&matches)
				if key == lm.subs[sdx].groups[g].keys[s] {
					value, ok = lm.subs[sdx].groups[g].values[s], true
					return
				}
			}
			// |key| is not in swissGroup |g|,
			// stop probing if we see an swissEmpty slot
			matches = swissMetaMatchEmpty(&lm.subs[sdx].ctrl[g])
			if matches != 0 {
				ok = false
				return
			}
			g += 1 // linear probing
			if g >= uint32(len(lm.subs[sdx].groups)) {
				g = 0
			}
		}
		//goland:noinspection GoUnreachableCode
		return
	}

	g := swissProbeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := swissMetaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := swissNextMatch(&matches)
			if key == m.groups[g].keys[s] {
				value, ok = m.groups[g].values[s], true
				return
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Put attempts to insert |key| and |value|
func (m *SwissMap[K, V]) Put(key K, value V) {
	hi, lo := swissSplitHash(m.hash.Hash64(key))
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		sdx := uint8(hi)
		if lm.subs[sdx].resident >= lm.subs[sdx].limit {
			m.subRehash(sdx, m.subNextSize(sdx))
		}

		g := swissProbeStart(hi, len(lm.subs[sdx].groups))
		for { // inlined find loop
			matches := swissMetaMatchH2(&lm.subs[sdx].ctrl[g], lo)
			for matches != 0 {
				s := swissNextMatch(&matches)
				if key == lm.subs[sdx].groups[g].keys[s] { // update
					lm.subs[sdx].groups[g].keys[s] = key
					lm.subs[sdx].groups[g].values[s] = value
					return
				}
			}
			// |key| is not in swissGroup |g|,
			// stop probing if we see an swissEmpty slot
			matches = swissMetaMatchEmpty(&lm.subs[sdx].ctrl[g])
			if matches != 0 { // insert
				s := swissNextMatch(&matches)
				lm.subs[sdx].groups[g].keys[s] = key
				lm.subs[sdx].groups[g].values[s] = value
				lm.subs[sdx].ctrl[g][s] = int8(lo)
				lm.subs[sdx].resident++
				return
			}
			g += 1 // linear probing
			if g >= uint32(len(lm.subs[sdx].groups)) {
				g = 0
			}
		}
		//goland:noinspection GoUnreachableCode
		return
	}

	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}

	g := swissProbeStart(hi, len(m.groups))
	for { // inlined find loop
		matches := swissMetaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := swissNextMatch(&matches)
			if key == m.groups[g].keys[s] { // update
				m.groups[g].keys[s] = key
				m.groups[g].values[s] = value
				return
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(&m.ctrl[g])
		if matches != 0 { // insert
			s := swissNextMatch(&matches)
			m.groups[g].keys[s] = key
			m.groups[g].values[s] = value
			m.ctrl[g][s] = int8(lo)
			m.resident++
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Delete attempts to remove |key|, returns true successful.
func (m *SwissMap[K, V]) Delete(key K) (ok bool) {
	hi, lo := swissSplitHash(m.hash.Hash64(key))
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		sdx := uint8(hi)

		g := swissProbeStart(hi, len(lm.subs[sdx].groups))
		for {
			matches := swissMetaMatchH2(&lm.subs[sdx].ctrl[g], lo)
			for matches != 0 {
				s := swissNextMatch(&matches)
				if key == lm.subs[sdx].groups[g].keys[s] {
					ok = true
					// optimization: if |m.ctrl[g]| contains any swissEmpty
					// swissMetadata bytes, we can physically delete |key|
					// rather than placing a swissTombstone.
					// The observation is that any probes into swissGroup |g|
					// would already be terminated by the existing swissEmpty
					// slot, and therefore reclaiming slot |s| will not
					// cause premature termination of probes into |g|.
					if swissMetaMatchEmpty(&lm.subs[sdx].ctrl[g]) != 0 {
						lm.subs[sdx].ctrl[g][s] = swissEmpty
						lm.subs[sdx].resident--
					} else {
						lm.subs[sdx].ctrl[g][s] = swissTombstone
						lm.subs[sdx].dead++
					}
					return
				}
			}
			// |key| is not in swissGroup |g|,
			// stop probing if we see an swissEmpty slot
			matches = swissMetaMatchEmpty(&lm.subs[sdx].ctrl[g])
			if matches != 0 { // |key| absent
				ok = false
				return
			}
			g += 1 // linear probing
			if g >= uint32(len(lm.subs[sdx].groups)) {
				g = 0
			}
		}
		//goland:noinspection GoUnreachableCode
		return
	}

	g := swissProbeStart(hi, len(m.groups))
	for {
		matches := swissMetaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s := swissNextMatch(&matches)
			if key == m.groups[g].keys[s] {
				ok = true
				// optimization: if |m.ctrl[g]| contains any swissEmpty
				// swissMetadata bytes, we can physically delete |key|
				// rather than placing a swissTombstone.
				// The observation is that any probes into swissGroup |g|
				// would already be terminated by the existing swissEmpty
				// slot, and therefore reclaiming slot |s| will not
				// cause premature termination of probes into |g|.
				if swissMetaMatchEmpty(&m.ctrl[g]) != 0 {
					m.ctrl[g][s] = swissEmpty
					m.resident--
				} else {
					m.ctrl[g][s] = swissTombstone
					m.dead++
				}
				return
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(&m.ctrl[g])
		if matches != 0 { // |key| absent
			ok = false
			return
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

// Iter iterates the elements of the SwissMap, passing them to the callback.
// It guarantees that any key in the SwissMap will be visited only once, and
// for un-mutated Maps, every key will be visited once. If the SwissMap is
// Mutated during iteration, mutations will be reflected on return from
// Iter, but the set of keys visited by Iter is non-deterministic.
func (m *SwissMap[K, V]) Iter(cb func(k K, v V) (stop bool)) {
	// take a consistent view of the table in case
	// we rehash during iteration
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		for sdx := uint32(0); sdx < splitSubMapSize; sdx++ {
			ctrl, groups := lm.subs[sdx].ctrl, lm.subs[sdx].groups
			// pick a random starting swissGroup
			g := swissRandIntN(len(groups))
			for n := 0; n < len(groups); n++ {
				for s, c := range ctrl[g] {
					if c == swissEmpty || c == swissTombstone {
						continue
					}
					k, v := groups[g].keys[s], groups[g].values[s]
					if stop := cb(k, v); stop {
						return
					}
				}
				g++
				if g >= uint32(len(groups)) {
					g = 0
				}
			}
		}
		return
	}

	ctrl, groups := m.ctrl, m.groups
	// pick a random starting swissGroup
	g := swissRandIntN(len(groups))
	for n := 0; n < len(groups); n++ {
		for s, c := range ctrl[g] {
			if c == swissEmpty || c == swissTombstone {
				continue
			}
			k, v := groups[g].keys[s], groups[g].values[s]
			if stop := cb(k, v); stop {
				return
			}
		}
		g++
		if g >= uint32(len(groups)) {
			g = 0
		}
	}
}

// Clear removes all elements from the SwissMap.
func (m *SwissMap[K, V]) Clear() {
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		for sdx := uint32(0); sdx < splitSubMapSize; sdx++ {
			t64 := (*[]uint64)(unsafe.Pointer(&lm.subs[sdx].ctrl))
			for i := range *t64 {
				(*t64)[i] = swissEmpty64
			}
			lm.subs[sdx].resident, lm.subs[sdx].dead = 0, 0
		}
		return
	}

	t64 := (*[]uint64)(unsafe.Pointer(&m.ctrl))
	for i := range *t64 {
		(*t64)[i] = swissEmpty64
	}
	m.resident, m.dead = 0, 0
}

// Count returns the number of elements in the SwissMap.
func (m *SwissMap[K, V]) Count() int {
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		var n = 0
		for sdx := uint32(0); sdx < splitSubMapSize; sdx++ {
			n += int(lm.subs[sdx].resident - lm.subs[sdx].dead)
		}
		return n
	}
	return int(m.resident - m.dead)
}

// Capacity returns the number of additional elements
// they can be added to the SwissMap before resizing.
func (m *SwissMap[K, V]) Capacity() int {
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		var n = 0
		for sdx := uint32(0); sdx < splitSubMapSize; sdx++ {
			n += int(lm.subs[sdx].limit - lm.subs[sdx].resident)
		}
		return n
	}
	return int(m.limit - m.resident)
}

// find returns the location of |key| if present, or its insertion location if absent.
// for performance, find is manually inlined into public methods.
func (m *SwissMap[K, V]) find(key K, hi swissH1, lo swissH2) (i int32, g, s uint32, ok bool) {
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		sdx := uint8(hi)

		g = swissProbeStart(hi, len(lm.subs[sdx].groups))
		for {
			matches := swissMetaMatchH2(&lm.subs[sdx].ctrl[g], lo)
			for matches != 0 {
				s = swissNextMatch(&matches)
				if key == lm.subs[sdx].groups[g].keys[s] {
					return int32(sdx), g, s, true
				}
			}
			// |key| is not in swissGroup |g|,
			// stop probing if we see an swissEmpty slot
			matches = swissMetaMatchEmpty(&lm.subs[sdx].ctrl[g])
			if matches != 0 {
				s = swissNextMatch(&matches)
				return int32(sdx), g, s, false
			}
			g += 1 // linear probing
			if g >= uint32(len(lm.subs[sdx].groups)) {
				g = 0
			}
		}
		//goland:noinspection GoUnreachableCode
		return
	}

	g = swissProbeStart(hi, len(m.groups))
	for {
		matches := swissMetaMatchH2(&m.ctrl[g], lo)
		for matches != 0 {
			s = swissNextMatch(&matches)
			if key == m.groups[g].keys[s] {
				return -1, g, s, true
			}
		}
		// |key| is not in swissGroup |g|,
		// stop probing if we see an swissEmpty slot
		matches = swissMetaMatchEmpty(&m.ctrl[g])
		if matches != 0 {
			s = swissNextMatch(&matches)
			return -1, g, s, false
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *SwissMap[K, V]) subNextSize(sdx uint8) (n uint32) {
	lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))

	n = uint32(len(lm.subs[sdx].groups)) * 2
	if lm.subs[sdx].dead >= (lm.subs[sdx].resident / 2) {
		n = uint32(len(lm.subs[sdx].groups))
	}
	return
}

func (m *SwissMap[K, V]) subRehash(sdx uint8, n uint32) {
	lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))

	groups, ctrl := lm.subs[sdx].groups, lm.subs[sdx].ctrl
	lm.subs[sdx].groups = make([]swissGroup[K, V], n)
	lm.subs[sdx].ctrl = make([]swissMetadata, n)

	t64 := (*[]uint64)(unsafe.Pointer(&lm.subs[sdx].ctrl))
	for i := range *t64 {
		(*t64)[i] = swissEmpty64
	}

	lm.subs[sdx].limit = n * swissMaxAvgGroupLoad
	lm.subs[sdx].resident, lm.subs[sdx].dead = 0, 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == swissEmpty || c == swissTombstone {
				continue
			}
			m.Put(groups[g].keys[s], groups[g].values[s])
		}
	}
}

func (m *SwissMap[K, V]) nextSize() (n uint32) {
	// flagLargeMap use sub nextSize
	n = uint32(len(m.groups)) * 2
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.groups))
	}
	return
}

func (m *SwissMap[K, V]) rehash(n uint32) {
	// flagLargeMap use sub rehash
	groups, ctrl := m.groups, m.ctrl
	m.groups = make([]swissGroup[K, V], n)
	m.ctrl = make([]swissMetadata, n)

	t64 := (*[]uint64)(unsafe.Pointer(&m.ctrl))
	for i := range *t64 {
		(*t64)[i] = swissEmpty64
	}

	m.limit = n * swissMaxAvgGroupLoad
	m.resident, m.dead = 0, 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == swissEmpty || c == swissTombstone {
				continue
			}
			m.Put(groups[g].keys[s], groups[g].values[s])
		}
	}
}

func (m *SwissMap[K, V]) loadFactor() float32 {
	slots := float32(len(m.groups) * swissGroupSize)
	return float32(m.resident-m.dead) / slots
}

func (m *SwissMap[K, V]) getResident() int {
	if m.flags == flagLargeMap {
		lm := (*SwissLargeMap[K, V])(unsafe.Pointer(m))
		var n = 0
		for sdx := uint32(0); sdx < splitSubMapSize; sdx++ {
			n += int(lm.subs[sdx].resident)
		}
		return n
	}
	return int(m.resident)
}

// swissNumGroups returns the minimum number of groups needed to store |n| elems.
func swissNumGroups(n uint32) (groups uint32) {
	groups = (n + swissMaxAvgGroupLoad - 1) / swissMaxAvgGroupLoad
	if groups == 0 {
		groups = 1
	}
	return
}

func swissNewEmptyMetadata() (meta swissMetadata) {
	for i := range meta {
		meta[i] = swissEmpty
	}
	return
}

func swissSplitHash(h uint64) (swissH1, swissH2) {
	return swissH1((h & swissH1Mask) >> 7), swissH2(h & swissH2Mask)
}

func swissProbeStart(hi swissH1, groups int) uint32 {
	return swissFastModN(uint32(hi), uint32(groups))
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func swissFastModN(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

// swissRandIntN returns a random number in the interval [0, n).
func swissRandIntN(n int) uint32 {
	return swissFastModN(fastrand(), uint32(n))
}

func swissMetaMatchH2(m *swissMetadata, h swissH2) uint64 {
	// https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	return swissHasZeroByte(*(*uint64)((unsafe.Pointer)(m)) ^ (swissLoBits * uint64(h)))
}

func swissMetaMatchEmpty(m *swissMetadata) uint64 {
	return swissHasZeroByte(*(*uint64)((unsafe.Pointer)(m)) ^ swissHiBits)
}

func swissNextMatch(b *uint64) uint32 {
	s := uint32(bits.TrailingZeros64(*b))
	*b &= ^(1 << s) // clear bit |s|
	return s >> 3   // div by 8
}

func swissHasZeroByte(x uint64) uint64 {
	return ((x - swissLoBits) & ^(x)) & swissHiBits
}

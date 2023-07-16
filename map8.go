package swiss

import (
	"github.com/dolthub/maphash"

	"math/bits"
	"unsafe"
)

const (
	groupSize8       = 8
	maxAvgGroupLoad8 = 7

	loBits8 uint64 = 0x0101010101010101
	hiBits8 uint64 = 0x8080808080808080
)

type bitset8 uint64

func metaMatchH8(m *metadata8, h h2E8) bitset8 {
	// https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	return hasZeroByte8(*(*uint64)((unsafe.Pointer)(m)) ^ (loBits8 * uint64(h)))
}

func metaMatchEmpty8(m *metadata8) bitset8 {
	return hasZeroByte8(*(*uint64)((unsafe.Pointer)(m)) ^ hiBits8)
}

func nextMatch8(b *bitset8) uint32 {
	s := uint32(bits.TrailingZeros64(uint64(*b)))
	*b &= ^(1 << s) // clear bit |s|
	return s >> 3   // div by 8
}

func hasZeroByte8(x uint64) bitset8 {
	return bitset8(((x - loBits8) & ^(x)) & hiBits8)
}

//go:linkname fastrand8 runtime.fastrand
func fastrand8() uint32

//goland:noinspection GoUnusedConst
const (
	maxLoadFactor8 = float32(maxAvgGroupLoad8) / float32(groupSize8)
)

// Map8 is an open-addressing hash map
// based on Abseil's flat_hash_map.
type Map8[K comparable, V any] struct {
	ctrl     []metadata8
	groups   []group8[K, V]
	hash     maphash.Hasher[K]
	resident uint32
	dead     uint32
	limit    uint32
}

// metadata8 is the h2E8 metadata8 array for a group8.
// find operations first probe the controls bytes
// to filter candidates before matching keys
type metadata8 [groupSize8]int8

// group8 is a group8 of 16 key-value pairs
type group8[K comparable, V any] struct {
	keys   [groupSize8]K
	values [groupSize8]V
}

const (
	h1Mask8    uint64 = 0xffff_ffff_ffff_ff80
	h2Mask8    uint64 = 0x0000_0000_0000_007f
	empty8     int8   = -128 // 0b1000_0000
	tombstone8 int8   = -2   // 0b1111_1110
)

// h1E8 is a 57 bit hash prefix
type h1E8 uint64

// h2E8 is a 7 bit hash suffix
type h2E8 int8

// NewMap8 constructs a Map8.
//
//goland:noinspection GoUnusedExportedFunction
func NewMap8[K comparable, V any](sz uint32) (m *Map8[K, V]) {
	groups := numGroups8(sz)
	m = &Map8[K, V]{
		ctrl:   make([]metadata8, groups),
		groups: make([]group8[K, V], groups),
		hash:   maphash.NewHasher[K](),
		limit:  groups * maxAvgGroupLoad8,
	}
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata8()
	}
	return
}

// Has returns true if |key| is present in |m|.
func (m *Map8[K, V]) Has(key K) (ok bool) {
	hi, lo := splitHash8(m.hash.Hash(key))
	g := probeStart8(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH8(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch8(&matches)
			if key == m.groups[g].keys[s] {
				ok = true
				return
			}
		}
		// |key| is not in group8 |g|,
		// stop probing if we see an empty8 slot
		matches = metaMatchEmpty8(&m.ctrl[g])
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
func (m *Map8[K, V]) Get(key K) (value V, ok bool) {
	hi, lo := splitHash8(m.hash.Hash(key))
	g := probeStart8(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH8(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch8(&matches)
			if key == m.groups[g].keys[s] {
				value, ok = m.groups[g].values[s], true
				return
			}
		}
		// |key| is not in group8 |g|,
		// stop probing if we see an empty8 slot
		matches = metaMatchEmpty8(&m.ctrl[g])
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
func (m *Map8[K, V]) Put(key K, value V) {
	if m.resident >= m.limit {
		m.rehash(m.nextSize())
	}
	hi, lo := splitHash8(m.hash.Hash(key))
	g := probeStart8(hi, len(m.groups))
	for { // inlined find loop
		matches := metaMatchH8(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch8(&matches)
			if key == m.groups[g].keys[s] { // update
				m.groups[g].keys[s] = key
				m.groups[g].values[s] = value
				return
			}
		}
		// |key| is not in group8 |g|,
		// stop probing if we see an empty8 slot
		matches = metaMatchEmpty8(&m.ctrl[g])
		if matches != 0 { // insert
			s := nextMatch8(&matches)
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
func (m *Map8[K, V]) Delete(key K) (ok bool) {
	hi, lo := splitHash8(m.hash.Hash(key))
	g := probeStart8(hi, len(m.groups))
	for {
		matches := metaMatchH8(&m.ctrl[g], lo)
		for matches != 0 {
			s := nextMatch8(&matches)
			if key == m.groups[g].keys[s] {
				ok = true
				// optimization: if |m.ctrl[g]| contains any empty8
				// metadata8 bytes, we can physically delete |key|
				// rather than placing a tombstone8.
				// The observation is that any probes into group8 |g|
				// would already be terminated by the existing empty8
				// slot, and therefore reclaiming slot |s| will not
				// cause premature termination of probes into |g|.
				if metaMatchEmpty8(&m.ctrl[g]) != 0 {
					m.ctrl[g][s] = empty8
					m.resident--
				} else {
					m.ctrl[g][s] = tombstone8
					m.dead++
				}
				return
			}
		}
		// |key| is not in group8 |g|,
		// stop probing if we see an empty8 slot
		matches = metaMatchEmpty8(&m.ctrl[g])
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

// Iter iterates the elements of the Map8, passing them to the callback.
// It guarantees that any key in the Map8 will be visited only once, and
// for un-mutated Maps, every key will be visited once. If the Map8 is
// Mutated during iteration, mutations will be reflected on return from
// Iter, but the set of keys visited by Iter is non-deterministic.
func (m *Map8[K, V]) Iter(cb func(k K, v V) (stop bool)) {
	// take a consistent view of the table in case
	// we rehash during iteration
	ctrl, groups := m.ctrl, m.groups
	// pick a random starting group8
	g := randIntN8(len(groups))
	for n := 0; n < len(groups); n++ {
		for s, c := range ctrl[g] {
			if c == empty8 || c == tombstone8 {
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

// Clear removes all elements from the Map8.
func (m *Map8[K, V]) Clear() {
	for i, c := range m.ctrl {
		for j := range c {
			m.ctrl[i][j] = empty8
		}
	}
	m.resident, m.dead = 0, 0
}

// Count returns the number of elements in the Map8.
func (m *Map8[K, V]) Count() int {
	return int(m.resident - m.dead)
}

// Capacity returns the number of additional elements
// they can be added to the Map8 before resizing.
func (m *Map8[K, V]) Capacity() int {
	return int(m.limit - m.resident)
}

// find returns the location of |key| if present, or its insertion location if absent.
// for performance, find is manually inlined into public methods.
func (m *Map8[K, V]) find(key K, hi h1E8, lo h2E8) (g, s uint32, ok bool) {
	g = probeStart8(hi, len(m.groups))
	for {
		matches := metaMatchH8(&m.ctrl[g], lo)
		for matches != 0 {
			s = nextMatch8(&matches)
			if key == m.groups[g].keys[s] {
				return g, s, true
			}
		}
		// |key| is not in group8 |g|,
		// stop probing if we see an empty8 slot
		matches = metaMatchEmpty8(&m.ctrl[g])
		if matches != 0 {
			s = nextMatch8(&matches)
			return g, s, false
		}
		g += 1 // linear probing
		if g >= uint32(len(m.groups)) {
			g = 0
		}
	}
}

func (m *Map8[K, V]) nextSize() (n uint32) {
	n = uint32(len(m.groups)) * 2
	if m.dead >= (m.resident / 2) {
		n = uint32(len(m.groups))
	}
	return
}

func (m *Map8[K, V]) rehash(n uint32) {
	groups, ctrl := m.groups, m.ctrl
	m.groups = make([]group8[K, V], n)
	m.ctrl = make([]metadata8, n)
	for i := range m.ctrl {
		m.ctrl[i] = newEmptyMetadata8()
	}
	m.hash = maphash.NewSeed(m.hash)
	m.limit = n * maxAvgGroupLoad8
	m.resident, m.dead = 0, 0
	for g := range ctrl {
		for s := range ctrl[g] {
			c := ctrl[g][s]
			if c == empty8 || c == tombstone8 {
				continue
			}
			m.Put(groups[g].keys[s], groups[g].values[s])
		}
	}
}

func (m *Map8[K, V]) loadFactor() float32 {
	slots := float32(len(m.groups) * groupSize8)
	return float32(m.resident-m.dead) / slots
}

// numGroups8 returns the minimum number of groups needed to store |n| elems.
func numGroups8(n uint32) (groups uint32) {
	groups = (n + maxAvgGroupLoad8 - 1) / maxAvgGroupLoad8
	if groups == 0 {
		groups = 1
	}
	return
}

func newEmptyMetadata8() (meta metadata8) {
	for i := range meta {
		meta[i] = empty8
	}
	return
}

func splitHash8(h uint64) (h1E8, h2E8) {
	return h1E8((h & h1Mask8) >> 7), h2E8(h & h2Mask8)
}

func probeStart8(hi h1E8, groups int) uint32 {
	return fastModN8(uint32(hi), uint32(groups))
}

// lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func fastModN8(x, n uint32) uint32 {
	return uint32((uint64(x) * uint64(n)) >> 32)
}

// randIntN8 returns a random number in the interval [0, n).
func randIntN8(n int) uint32 {
	return fastModN8(fastrand8(), uint32(n))
}

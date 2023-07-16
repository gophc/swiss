package swiss

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestMatchMetadata8(t *testing.T) {
	var meta metadata8
	for i := range meta {
		meta[i] = int8(i)
	}
	t.Run("metaMatchH2E8", func(t *testing.T) {
		for _, x := range meta {
			mask := metaMatchH2E8(&meta, h2E8(x))
			assert.NotZero(t, mask)
			assert.Equal(t, uint32(x), nextMatch8(&mask))
		}
	})
	t.Run("metaMatchEmpty8", func(t *testing.T) {
		mask := metaMatchEmpty8(&meta)
		assert.Equal(t, mask, bitset8(0))
		for i := range meta {
			meta[i] = empty8
			mask = metaMatchEmpty8(&meta)
			assert.NotZero(t, mask)
			assert.Equal(t, uint32(i), nextMatch8(&mask))
			meta[i] = int8(i)
		}
	})
	t.Run("nextMatch8", func(t *testing.T) {
		// test iterating multiple matches
		meta = newEmptyMetadata8()
		mask := metaMatchEmpty8(&meta)
		for i := range meta {
			assert.Equal(t, uint32(i), nextMatch8(&mask))
		}
		for i := 0; i < len(meta); i += 2 {
			meta[i] = int8(42)
		}
		mask = metaMatchH2E8(&meta, h2E8(42))
		for i := 0; i < len(meta); i += 2 {
			assert.Equal(t, uint32(i), nextMatch8(&mask))
		}
	})
}

func BenchmarkMatchMetadata8(b *testing.B) {
	var meta metadata8
	for i := range meta {
		meta[i] = int8(i)
	}
	var mask bitset8
	for i := 0; i < b.N; i++ {
		mask = metaMatchH2E8(&meta, h2E8(i))
	}
	b.Log(mask)
}

func TestNextPow2E8(t *testing.T) {
	assert.Equal(t, 0, int(nextPow2E8(0)))
	assert.Equal(t, 1, int(nextPow2E8(1)))
	assert.Equal(t, 2, int(nextPow2E8(2)))
	assert.Equal(t, 4, int(nextPow2E8(3)))
	assert.Equal(t, 8, int(nextPow2E8(7)))
	assert.Equal(t, 8, int(nextPow2E8(8)))
	assert.Equal(t, 16, int(nextPow2E8(9)))
}

func nextPow2E8(x uint32) uint32 {
	return 1 << (32 - bits.LeadingZeros32(x-1))
}

func TestConstants8(t *testing.T) {
	c1, c2 := empty8, tombstone8
	assert.Equal(t, byte(0b1000_0000), byte(c1))
	assert.Equal(t, byte(0b1000_0000), reinterpretCast8(c1))
	assert.Equal(t, byte(0b1111_1110), byte(c2))
	assert.Equal(t, byte(0b1111_1110), reinterpretCast8(c2))
}

func reinterpretCast8(i int8) byte {
	return *(*byte)(unsafe.Pointer(&i))
}

func TestFastMod8(t *testing.T) {
	t.Run("n=10", func(t *testing.T) {
		testFastMod8(t, 10)
	})
	t.Run("n=100", func(t *testing.T) {
		testFastMod8(t, 100)
	})
	t.Run("n=1000", func(t *testing.T) {
		testFastMod8(t, 1000)
	})
}

func testFastMod8(t *testing.T, n uint32) {
	const trials = 32 * 1024
	for i := 0; i < trials; i++ {
		x := rand.Uint32()
		y := fastModN8(x, n)
		assert.Less(t, y, n)
		t.Logf("fastMod8(%d, %d): %d", x, n, y)
	}
}

func TestSwissMap8(t *testing.T) {
	t.Run("strings=0", func(t *testing.T) {
		testSwissMap8(t, genStringData8(16, 0))
	})
	t.Run("strings=100", func(t *testing.T) {
		testSwissMap8(t, genStringData8(16, 100))
	})
	t.Run("strings=1000", func(t *testing.T) {
		testSwissMap8(t, genStringData8(16, 1000))
	})
	t.Run("strings=10_000", func(t *testing.T) {
		testSwissMap8(t, genStringData8(16, 10_000))
	})
	t.Run("strings=100_000", func(t *testing.T) {
		testSwissMap8(t, genStringData8(16, 100_000))
	})
	t.Run("uint32=0", func(t *testing.T) {
		testSwissMap8(t, genUint32Data8(0))
	})
	t.Run("uint32=100", func(t *testing.T) {
		testSwissMap8(t, genUint32Data8(100))
	})
	t.Run("uint32=1000", func(t *testing.T) {
		testSwissMap8(t, genUint32Data8(1000))
	})
	t.Run("uint32=10_000", func(t *testing.T) {
		testSwissMap8(t, genUint32Data8(10_000))
	})
	t.Run("uint32=100_000", func(t *testing.T) {
		testSwissMap8(t, genUint32Data8(100_000))
	})
	t.Run("string capacity", func(t *testing.T) {
		testSwissMapCapacity8(t, func(n int) []string {
			return genStringData8(16, n)
		})
	})
	t.Run("uint32 capacity", func(t *testing.T) {
		testSwissMapCapacity8(t, genUint32Data8)
	})
}

func testSwissMap8[K comparable](t *testing.T, keys []K) {
	// sanity check
	require.Equal(t, len(keys), len(uniq8(keys)), keys)
	t.Run("put", func(t *testing.T) {
		testMapPut8(t, keys)
	})
	t.Run("has", func(t *testing.T) {
		testMapHas8(t, keys)
	})
	t.Run("get", func(t *testing.T) {
		testMapGet8(t, keys)
	})
	t.Run("delete", func(t *testing.T) {
		testMapDelete8(t, keys)
	})
	t.Run("clear", func(t *testing.T) {
		testMapClear8(t, keys)
	})
	t.Run("iter", func(t *testing.T) {
		testMapIter8(t, keys)
	})
	t.Run("grow", func(t *testing.T) {
		testMapGrow8(t, keys)
	})
	t.Run("probe stats", func(t *testing.T) {
		testProbeStats8(t, keys)
	})
}

func uniq8[K comparable](keys []K) []K {
	s := make(map[K]struct{}, len(keys))
	for _, k := range keys {
		s[k] = struct{}{}
	}
	u := make([]K, 0, len(keys))
	for k := range s {
		u = append(u, k)
	}
	return u
}

func genStringData8(size, count int) (keys []string) {
	src := rand.New(rand.NewSource(int64(size * count)))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r := make([]rune, size*count)
	for i := range r {
		r[i] = letters[src.Intn(len(letters))]
	}
	keys = make([]string, count)
	for i := range keys {
		keys[i] = string(r[:size])
		r = r[size:]
	}
	return
}

func genUint32Data8(count int) (keys []uint32) {
	keys = make([]uint32, count)
	var x uint32
	for i := range keys {
		x += (rand.Uint32() % 128) + 1
		keys[i] = x
	}
	return
}

func testMapPut8[K comparable](t *testing.T, keys []K) {
	m := NewMap8[K, int](uint32(len(keys)))
	assert.Equal(t, 0, m.Count())
	for i, key := range keys {
		m.Put(key, i)
	}
	assert.Equal(t, len(keys), m.Count())
	// overwrite
	for i, key := range keys {
		m.Put(key, -i)
	}
	assert.Equal(t, len(keys), m.Count())
	for i, key := range keys {
		act, ok := m.Get(key)
		assert.True(t, ok)
		assert.Equal(t, -i, act)
	}
	assert.Equal(t, len(keys), int(m.resident))
}

func testMapHas8[K comparable](t *testing.T, keys []K) {
	m := NewMap8[K, int](uint32(len(keys)))
	for i, key := range keys {
		m.Put(key, i)
	}
	for _, key := range keys {
		ok := m.Has(key)
		assert.True(t, ok)
	}
}

func testMapGet8[K comparable](t *testing.T, keys []K) {
	m := NewMap8[K, int](uint32(len(keys)))
	for i, key := range keys {
		m.Put(key, i)
	}
	for i, key := range keys {
		act, ok := m.Get(key)
		assert.True(t, ok)
		assert.Equal(t, i, act)
	}
}

func testMapDelete8[K comparable](t *testing.T, keys []K) {
	m := NewMap8[K, int](uint32(len(keys)))
	assert.Equal(t, 0, m.Count())
	for i, key := range keys {
		m.Put(key, i)
	}
	assert.Equal(t, len(keys), m.Count())
	for _, key := range keys {
		m.Delete(key)
		ok := m.Has(key)
		assert.False(t, ok)
	}
	assert.Equal(t, 0, m.Count())
	// put keys back after deleting them
	for i, key := range keys {
		m.Put(key, i)
	}
	assert.Equal(t, len(keys), m.Count())
}

func testMapClear8[K comparable](t *testing.T, keys []K) {
	m := NewMap8[K, int](0)
	assert.Equal(t, 0, m.Count())
	for i, key := range keys {
		m.Put(key, i)
	}
	assert.Equal(t, len(keys), m.Count())
	m.Clear()
	assert.Equal(t, 0, m.Count())
	for _, key := range keys {
		ok := m.Has(key)
		assert.False(t, ok)
		_, ok = m.Get(key)
		assert.False(t, ok)
	}
	var calls int
	m.Iter(func(k K, v int) (stop bool) {
		calls++
		return
	})
	assert.Equal(t, 0, calls)
}

func testMapIter8[K comparable](t *testing.T, keys []K) {
	m := NewMap8[K, int](uint32(len(keys)))
	for i, key := range keys {
		m.Put(key, i)
	}
	visited := make(map[K]uint, len(keys))
	m.Iter(func(k K, v int) (stop bool) {
		visited[k] = 0
		stop = true
		return
	})
	if len(keys) == 0 {
		assert.Equal(t, len(visited), 0)
	} else {
		assert.Equal(t, len(visited), 1)
	}
	for _, k := range keys {
		visited[k] = 0
	}
	m.Iter(func(k K, v int) (stop bool) {
		visited[k]++
		return
	})
	for _, c := range visited {
		assert.Equal(t, c, uint(1))
	}
	// mutate on iter
	m.Iter(func(k K, v int) (stop bool) {
		m.Put(k, -v)
		return
	})
	for i, key := range keys {
		act, ok := m.Get(key)
		assert.True(t, ok)
		assert.Equal(t, -i, act)
	}
}

func testMapGrow8[K comparable](t *testing.T, keys []K) {
	n := uint32(len(keys))
	m := NewMap8[K, int](n / 10)
	for i, key := range keys {
		m.Put(key, i)
	}
	for i, key := range keys {
		act, ok := m.Get(key)
		assert.True(t, ok)
		assert.Equal(t, i, act)
	}
}

func testSwissMapCapacity8[K comparable](t *testing.T, gen func(n int) []K) {
	// Capacity() behavior depends on |groupSize|
	// which varies by processor architecture.
	caps := []uint32{
		1 * maxAvgGroupLoad8,
		2 * maxAvgGroupLoad8,
		3 * maxAvgGroupLoad8,
		4 * maxAvgGroupLoad8,
		5 * maxAvgGroupLoad8,
		10 * maxAvgGroupLoad8,
		25 * maxAvgGroupLoad8,
		50 * maxAvgGroupLoad8,
		100 * maxAvgGroupLoad8,
	}
	for _, c := range caps {
		m := NewMap8[K, K](c)
		assert.Equal(t, int(c), m.Capacity())
		keys := gen(rand.Intn(int(c)))
		for _, k := range keys {
			m.Put(k, k)
		}
		assert.Equal(t, int(c)-len(keys), m.Capacity())
		assert.Equal(t, int(c), m.Count()+m.Capacity())
	}
}

func testProbeStats8[K comparable](t *testing.T, keys []K) {
	runTest := func(load float32) {
		n := uint32(len(keys))
		sz, k := loadFactorSample8(n, load)
		m := NewMap8[K, int](sz)
		for i, key := range keys[:k] {
			m.Put(key, i)
		}
		// todo: assert stat invariants?
		stats := getProbeStats8(t, m, keys)
		t.Log(fmtProbeStats8(stats))
	}
	t.Run("load_factor=0.5", func(t *testing.T) {
		runTest(0.5)
	})
	t.Run("load_factor=0.75", func(t *testing.T) {
		runTest(0.75)
	})
	t.Run("load_factor=max", func(t *testing.T) {
		runTest(maxLoadFactor8)
	})
}

// calculates the sample size and map size necessary to
// create a load factor of |load| given |n| data points
func loadFactorSample8(n uint32, targetLoad float32) (mapSz, sampleSz uint32) {
	if targetLoad > maxLoadFactor8 {
		targetLoad = maxLoadFactor8
	}
	// tables are assumed to be power of two
	sampleSz = uint32(float32(n) * targetLoad)
	mapSz = uint32(float32(n) * maxLoadFactor8)
	return
}

type probeStats8 struct {
	groups     uint32
	loadFactor float32
	presentCnt uint32
	presentMin uint32
	presentMax uint32
	presentAvg float32
	absentCnt  uint32
	absentMin  uint32
	absentMax  uint32
	absentAvg  float32
}

func fmtProbeStats8(s probeStats8) string {
	g := fmt.Sprintf("groups=%d load=%f\n", s.groups, s.loadFactor)
	p := fmt.Sprintf("present(n=%d): min=%d max=%d avg=%f\n",
		s.presentCnt, s.presentMin, s.presentMax, s.presentAvg)
	a := fmt.Sprintf("absent(n=%d):  min=%d max=%d avg=%f\n",
		s.absentCnt, s.absentMin, s.absentMax, s.absentAvg)
	return g + p + a
}

func getProbeLength8[K comparable, V any](t *testing.T, m *Map8[K, V], key K) (length uint32, ok bool) {
	var end uint32
	hi, lo := splitHash8(m.hash.Hash(key))
	start := probeStart8(hi, len(m.groups))
	end, _, ok = m.find(key, hi, lo)
	if end < start { // wrapped
		end += uint32(len(m.groups))
	}
	length = (end - start) + 1
	require.True(t, length > 0)
	return
}

func getProbeStats8[K comparable, V any](t *testing.T, m *Map8[K, V], keys []K) (stats probeStats8) {
	stats.groups = uint32(len(m.groups))
	stats.loadFactor = m.loadFactor()
	var presentSum, absentSum float32
	stats.presentMin = math.MaxInt32
	stats.absentMin = math.MaxInt32
	for _, key := range keys {
		l, ok := getProbeLength8(t, m, key)
		if ok {
			stats.presentCnt++
			presentSum += float32(l)
			if stats.presentMin > l {
				stats.presentMin = l
			}
			if stats.presentMax < l {
				stats.presentMax = l
			}
		} else {
			stats.absentCnt++
			absentSum += float32(l)
			if stats.absentMin > l {
				stats.absentMin = l
			}
			if stats.absentMax < l {
				stats.absentMax = l
			}
		}
	}
	if stats.presentCnt == 0 {
		stats.presentMin = 0
	} else {
		stats.presentAvg = presentSum / float32(stats.presentCnt)
	}
	if stats.absentCnt == 0 {
		stats.absentMin = 0
	} else {
		stats.absentAvg = absentSum / float32(stats.absentCnt)
	}
	return
}

func TestNumGroups8(t *testing.T) {
	assert.Equal(t, expected8(0), numGroups8(0))
	assert.Equal(t, expected8(1), numGroups8(1))
	// max load factor 0.875
	assert.Equal(t, expected8(14), numGroups8(14))
	assert.Equal(t, expected8(15), numGroups8(15))
	assert.Equal(t, expected8(28), numGroups8(28))
	assert.Equal(t, expected8(29), numGroups8(29))
	assert.Equal(t, expected8(56), numGroups8(56))
	assert.Equal(t, expected8(57), numGroups8(57))
}

func expected8(x int) (groups uint32) {
	groups = uint32(math.Ceil(float64(x) / float64(maxAvgGroupLoad8)))
	if groups == 0 {
		groups = 1
	}
	return
}

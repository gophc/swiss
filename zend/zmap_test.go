package zend

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

func TestSwissMatchMetadata(t *testing.T) {
	var meta swissMetadata
	for i := range meta {
		meta[i] = int8(i)
	}
	t.Run("swissMetaMatchH2", func(t *testing.T) {
		for _, x := range meta {
			mask := swissMetaMatchH2(&meta, swissH2(x))
			assert.NotZero(t, mask)
			assert.Equal(t, uint32(x), swissNextMatch(&mask))
		}
	})
	t.Run("swissMetaMatchEmpty", func(t *testing.T) {
		mask := swissMetaMatchEmpty(&meta)
		assert.Equal(t, mask, swissBitset(0))
		for i := range meta {
			meta[i] = swissEmpty
			mask = swissMetaMatchEmpty(&meta)
			assert.NotZero(t, mask)
			assert.Equal(t, uint32(i), swissNextMatch(&mask))
			meta[i] = int8(i)
		}
	})
	t.Run("swissNextMatch", func(t *testing.T) {
		// test iterating multiple matches
		meta = swissNewEmptyMetadata()
		mask := swissMetaMatchEmpty(&meta)
		for i := range meta {
			assert.Equal(t, uint32(i), swissNextMatch(&mask))
		}
		for i := 0; i < len(meta); i += 2 {
			meta[i] = int8(42)
		}
		mask = swissMetaMatchH2(&meta, swissH2(42))
		for i := 0; i < len(meta); i += 2 {
			assert.Equal(t, uint32(i), swissNextMatch(&mask))
		}
	})
}

func BenchmarkSwissMatchMetadata(b *testing.B) {
	var meta swissMetadata
	for i := range meta {
		meta[i] = int8(i)
	}
	var mask swissBitset
	for i := 0; i < b.N; i++ {
		mask = swissMetaMatchH2(&meta, swissH2(i))
	}
	b.Log(mask)
}

func TestSwissNextPow2(t *testing.T) {
	assert.Equal(t, 0, int(swissNextPow2(0)))
	assert.Equal(t, 1, int(swissNextPow2(1)))
	assert.Equal(t, 2, int(swissNextPow2(2)))
	assert.Equal(t, 4, int(swissNextPow2(3)))
	assert.Equal(t, 8, int(swissNextPow2(7)))
	assert.Equal(t, 8, int(swissNextPow2(8)))
	assert.Equal(t, 16, int(swissNextPow2(9)))
}

func swissNextPow2(x uint32) uint32 {
	return 1 << (32 - bits.LeadingZeros32(x-1))
}

func TestSwissConstants(t *testing.T) {
	c1, c2 := swissEmpty, swissTombstone
	assert.Equal(t, byte(0b1000_0000), byte(c1))
	assert.Equal(t, byte(0b1000_0000), swissReinterpretCast(c1))
	assert.Equal(t, byte(0b1111_1110), byte(c2))
	assert.Equal(t, byte(0b1111_1110), swissReinterpretCast(c2))
}

func swissReinterpretCast(i int8) byte {
	return *(*byte)(unsafe.Pointer(&i))
}

func TestSwissFastMod(t *testing.T) {
	t.Run("n=10", func(t *testing.T) {
		testSwissFastMod(t, 10)
	})
	t.Run("n=100", func(t *testing.T) {
		testSwissFastMod(t, 100)
	})
	t.Run("n=1000", func(t *testing.T) {
		testSwissFastMod(t, 1000)
	})
}

func testSwissFastMod(t *testing.T, n uint32) {
	const trials = 32 * 1024
	for i := 0; i < trials; i++ {
		x := rand.Uint32()
		y := swissFastModN(x, n)
		assert.Less(t, y, n)
		t.Logf("swissFastModN(%d, %d): %d", x, n, y)
	}
}

func TestSwissMap(t *testing.T) {
	t.Run("strings=0", func(t *testing.T) {
		testSwissMap(t, genSwissStringData(16, 0))
	})
	t.Run("strings=100", func(t *testing.T) {
		testSwissMap(t, genSwissStringData(16, 100))
	})
	t.Run("strings=1000", func(t *testing.T) {
		testSwissMap(t, genSwissStringData(16, 1000))
	})
	t.Run("strings=10_000", func(t *testing.T) {
		testSwissMap(t, genSwissStringData(16, 10_000))
	})
	t.Run("strings=100_000", func(t *testing.T) {
		testSwissMap(t, genSwissStringData(16, 100_000))
	})
	t.Run("uint32=0", func(t *testing.T) {
		testSwissMap(t, genSwissUint32Data(0))
	})
	t.Run("uint32=100", func(t *testing.T) {
		testSwissMap(t, genSwissUint32Data(100))
	})
	t.Run("uint32=1000", func(t *testing.T) {
		testSwissMap(t, genSwissUint32Data(1000))
	})
	t.Run("uint32=10_000", func(t *testing.T) {
		testSwissMap(t, genSwissUint32Data(10_000))
	})
	t.Run("uint32=100_000", func(t *testing.T) {
		testSwissMap(t, genSwissUint32Data(100_000))
	})
	t.Run("string capacity", func(t *testing.T) {
		testSwissMapCapacity(t, func(n int) []string {
			return genSwissStringData(16, n)
		})
	})
	t.Run("uint32 capacity", func(t *testing.T) {
		testSwissMapCapacity(t, genSwissUint32Data)
	})
}

func testSwissMap[K comparable](t *testing.T, keys []K) {
	// sanity check
	require.Equal(t, len(keys), len(swissUniq(keys)), keys)
	t.Run("put", func(t *testing.T) {
		testSwissMapPut(t, keys)
	})
	t.Run("has", func(t *testing.T) {
		testSwissMapHas(t, keys)
	})
	t.Run("get", func(t *testing.T) {
		testSwissMapGet(t, keys)
	})
	t.Run("delete", func(t *testing.T) {
		testSwissMapDelete(t, keys)
	})
	t.Run("clear", func(t *testing.T) {
		testSwissMapClear(t, keys)
	})
	t.Run("iter", func(t *testing.T) {
		testSwissMapIter(t, keys)
	})
	t.Run("grow", func(t *testing.T) {
		testSwissMapGrow(t, keys)
	})
	t.Run("probe stats", func(t *testing.T) {
		testSwissProbeStats(t, keys)
	})
}

func swissUniq[K comparable](keys []K) []K {
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

func genSwissStringData(size, count int) (keys []string) {
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

func genSwissUint32Data(count int) (keys []uint32) {
	keys = make([]uint32, count)
	var x uint32
	for i := range keys {
		x += (rand.Uint32() % 128) + 1
		keys[i] = x
	}
	return
}

func testSwissMapPut[K comparable](t *testing.T, keys []K) {
	m := NewSwissMap[K, int](uint32(len(keys)))
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

func testSwissMapHas[K comparable](t *testing.T, keys []K) {
	m := NewSwissMap[K, int](uint32(len(keys)))
	for i, key := range keys {
		m.Put(key, i)
	}
	for _, key := range keys {
		ok := m.Has(key)
		assert.True(t, ok)
	}
}

func testSwissMapGet[K comparable](t *testing.T, keys []K) {
	m := NewSwissMap[K, int](uint32(len(keys)))
	for i, key := range keys {
		m.Put(key, i)
	}
	for i, key := range keys {
		act, ok := m.Get(key)
		assert.True(t, ok)
		assert.Equal(t, i, act)
	}
}

func testSwissMapDelete[K comparable](t *testing.T, keys []K) {
	m := NewSwissMap[K, int](uint32(len(keys)))
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

func testSwissMapClear[K comparable](t *testing.T, keys []K) {
	m := NewSwissMap[K, int](0)
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

func testSwissMapIter[K comparable](t *testing.T, keys []K) {
	m := NewSwissMap[K, int](uint32(len(keys)))
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

func testSwissMapGrow[K comparable](t *testing.T, keys []K) {
	n := uint32(len(keys))
	m := NewSwissMap[K, int](n / 10)
	for i, key := range keys {
		m.Put(key, i)
	}
	for i, key := range keys {
		act, ok := m.Get(key)
		assert.True(t, ok)
		assert.Equal(t, i, act)
	}
}

func testSwissMapCapacity[K comparable](t *testing.T, gen func(n int) []K) {
	// Capacity() behavior depends on |groupSize|
	// which varies by processor architecture.
	caps := []uint32{
		1 * swissMaxAvgGroupLoad,
		2 * swissMaxAvgGroupLoad,
		3 * swissMaxAvgGroupLoad,
		4 * swissMaxAvgGroupLoad,
		5 * swissMaxAvgGroupLoad,
		10 * swissMaxAvgGroupLoad,
		25 * swissMaxAvgGroupLoad,
		50 * swissMaxAvgGroupLoad,
		100 * swissMaxAvgGroupLoad,
	}
	for _, c := range caps {
		m := NewSwissMap[K, K](c)
		assert.Equal(t, int(c), m.Capacity())
		keys := gen(rand.Intn(int(c)))
		for _, k := range keys {
			m.Put(k, k)
		}
		assert.Equal(t, int(c)-len(keys), m.Capacity())
		assert.Equal(t, int(c), m.Count()+m.Capacity())
	}
}

func testSwissProbeStats[K comparable](t *testing.T, keys []K) {
	runTest := func(load float32) {
		n := uint32(len(keys))
		sz, k := swissLoadFactorSample(n, load)
		m := NewSwissMap[K, int](sz)
		for i, key := range keys[:k] {
			m.Put(key, i)
		}
		// todo: assert stat invariants?
		stats := swissGetProbeStats(t, m, keys)
		t.Log(swissFmtProbeStats(stats))
	}
	t.Run("load_factor=0.5", func(t *testing.T) {
		runTest(0.5)
	})
	t.Run("load_factor=0.75", func(t *testing.T) {
		runTest(0.75)
	})
	t.Run("load_factor=max", func(t *testing.T) {
		runTest(swissMaxLoadFactor)
	})
}

// calculates the sample size and map size necessary to
// create a load factor of |load| given |n| data points
func swissLoadFactorSample(n uint32, targetLoad float32) (mapSz, sampleSz uint32) {
	if targetLoad > swissMaxLoadFactor {
		targetLoad = swissMaxLoadFactor
	}
	// tables are assumed to be power of two
	sampleSz = uint32(float32(n) * targetLoad)
	mapSz = uint32(float32(n) * swissMaxLoadFactor)
	return
}

type swissProbeStats struct {
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

func swissFmtProbeStats(s swissProbeStats) string {
	g := fmt.Sprintf("groups=%d load=%f\n", s.groups, s.loadFactor)
	p := fmt.Sprintf("present(n=%d): min=%d max=%d avg=%f\n",
		s.presentCnt, s.presentMin, s.presentMax, s.presentAvg)
	a := fmt.Sprintf("absent(n=%d):  min=%d max=%d avg=%f\n",
		s.absentCnt, s.absentMin, s.absentMax, s.absentAvg)
	return g + p + a
}

func swissGetProbeLength[K comparable, V any](t *testing.T, m *SwissMap[K, V], key K) (length uint32, ok bool) {
	var end uint32
	hi, lo := swissSplitHash(m.hash.Hash64(key))
	start := swissProbeStart(hi, len(m.groups))
	end, _, ok = m.find(key, hi, lo)
	if end < start { // wrapped
		end += uint32(len(m.groups))
	}
	length = (end - start) + 1
	require.True(t, length > 0)
	return
}

func swissGetProbeStats[K comparable, V any](t *testing.T, m *SwissMap[K, V], keys []K) (stats swissProbeStats) {
	stats.groups = uint32(len(m.groups))
	stats.loadFactor = m.loadFactor()
	var presentSum, absentSum float32
	stats.presentMin = math.MaxInt32
	stats.absentMin = math.MaxInt32
	for _, key := range keys {
		l, ok := swissGetProbeLength(t, m, key)
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

func TestSwissNumGroups(t *testing.T) {
	assert.Equal(t, swissExpected(0), swissNumGroups(0))
	assert.Equal(t, swissExpected(1), swissNumGroups(1))
	// max load factor 0.875
	assert.Equal(t, swissExpected(14), swissNumGroups(14))
	assert.Equal(t, swissExpected(15), swissNumGroups(15))
	assert.Equal(t, swissExpected(28), swissNumGroups(28))
	assert.Equal(t, swissExpected(29), swissNumGroups(29))
	assert.Equal(t, swissExpected(56), swissNumGroups(56))
	assert.Equal(t, swissExpected(57), swissNumGroups(57))
}

func swissExpected(x int) (groups uint32) {
	groups = uint32(math.Ceil(float64(x) / float64(swissMaxAvgGroupLoad)))
	if groups == 0 {
		groups = 1
	}
	return
}

package swiss

import (
	"github.com/dolthub/swiss/minswiss"
	"github.com/dolthub/swiss/zend"
	"github.com/stretchr/testify/require"
	"math/bits"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSwissMinMap(t *testing.T) {
	t.Run("strings=0", func(t *testing.T) {
		testSwissMap8(t, genStringData8(16, 0))
	})
	t.Run("strings=100", func(t *testing.T) {
		testSwissMinMap(t, genStringData8(16, 100))
	})
	t.Run("strings=1000", func(t *testing.T) {
		testSwissMinMap(t, genStringData8(16, 1000))
	})
	t.Run("strings=10_000", func(t *testing.T) {
		testSwissMinMap(t, genStringData8(16, 10_000))
	})
	t.Run("strings=100_000", func(t *testing.T) {
		testSwissMinMap(t, genStringData8(16, 100_000))
	})
	t.Run("uint32=0", func(t *testing.T) {
		testSwissMinMap(t, genUint32Data8(0))
	})
	t.Run("uint32=100", func(t *testing.T) {
		testSwissMinMap(t, genUint32Data8(100))
	})
	t.Run("uint32=1000", func(t *testing.T) {
		testSwissMinMap(t, genUint32Data8(1000))
	})
	t.Run("uint32=10_000", func(t *testing.T) {
		testSwissMinMap(t, genUint32Data8(10_000))
	})
	t.Run("uint32=100_000", func(t *testing.T) {
		testSwissMinMap(t, genUint32Data8(100_000))
	})
}

//region TestSwissMinMap help func

func testSwissMinMap[K comparable](t *testing.T, keys []K) {
	// sanity check
	require.Equal(t, len(keys), len(uniq8(keys)), keys)
	t.Run("put", func(t *testing.T) {
		testSwissMinMapPut(t, keys)
	})
	t.Run("get", func(t *testing.T) {
		testSwissMinMapGet(t, keys)
	})
}

func testSwissMinMapPut[K comparable](t *testing.T, keys []K) {
	m := minswiss.NewSwissMap[K, int](uint32(len(keys)))
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
	assert.Equal(t, len(keys), int(m.GetResident()))
	assert.True(t, m.UnnecessaryCmp >= 0)
}

func testSwissMinMapGet[K comparable](t *testing.T, keys []K) {
	m := minswiss.NewSwissMap[K, int](uint32(len(keys)))
	for i, key := range keys {
		m.Put(key, i)
	}
	for i, key := range keys {
		act, ok := m.Get(key)
		assert.True(t, ok)
		assert.Equal(t, i, act)
	}
	assert.True(t, m.UnnecessaryCmp >= 0)
}

//endregion

func TestZMap(t *testing.T) {
	t.Run("strings=0", func(t *testing.T) {
		testZMapStr(t, genStringData8(16, 0))
	})
	t.Run("strings=100", func(t *testing.T) {
		testZMapStr(t, genStringData8(16, 100))
	})
	t.Run("strings=1000", func(t *testing.T) {
		testZMapStr(t, genStringData8(16, 1000))
	})
	t.Run("strings=10_000", func(t *testing.T) {
		testZMapStr(t, genStringData8(16, 10_000))
	})
	t.Run("strings=100_000", func(t *testing.T) {
		testZMapStr(t, genStringData8(16, 100_000))
	})
	t.Run("uint32=0", func(t *testing.T) {
		testZMap(t, genUint32Data8(0))
	})
	t.Run("uint32=100", func(t *testing.T) {
		testZMap(t, genUint32Data8(100))
	})
	t.Run("uint32=1000", func(t *testing.T) {
		testZMap(t, genUint32Data8(1000))
	})
	t.Run("uint32=10_000", func(t *testing.T) {
		testZMap(t, genUint32Data8(10_000))
	})
	t.Run("uint32=100_000", func(t *testing.T) {
		testZMap(t, genUint32Data8(100_000))
	})
}

//region TestZMap help func

func testZMap(t *testing.T, keys []uint32) {
	// sanity check
	require.Equal(t, len(keys), len(uniq8(keys)), keys)
	t.Run("put", func(t *testing.T) {
		testZMapPut(t, keys)
	})
	t.Run("get", func(t *testing.T) {
		testZMapGet(t, keys)
	})
}

func testZMapPut(t *testing.T, keys []uint32) {
	m := zend.NewZMap(uint32(len(keys)))
	assert.Equal(t, 0, m.Count())
	for i, key := range keys {
		m.Put(zend.TZvalLong(int64(key)), zend.TZvalLong(int64(i)))
	}
	assert.Equal(t, len(keys), m.Count())
	// overwrite
	for i, key := range keys {
		m.Put(zend.TZvalLong(int64(key)), zend.TZvalLong(int64(-i)))
	}
	assert.Equal(t, len(keys), m.Count())
	for i, key := range keys {
		act, ok := m.Get(zend.TZvalLong(int64(key)))
		assert.True(t, ok)
		assert.Equal(t, int64(-i), act.TZLong_())
	}
	assert.Equal(t, len(keys), int(m.GetResident()))
}

func testZMapGet(t *testing.T, keys []uint32) {
	m := zend.NewZMap(uint32(len(keys)))
	for i, key := range keys {
		m.Put(zend.TZvalLong(int64(key)), zend.TZvalLong(int64(i)))
	}
	for i, key := range keys {
		act, ok := m.Get(zend.TZvalLong(int64(key)))
		assert.True(t, ok)
		assert.Equal(t, int64(i), act.TZLong_())
	}
}

func testZMapStr(t *testing.T, keys []string) {
	// sanity check
	require.Equal(t, len(keys), len(uniq8(keys)), keys)
	t.Run("put", func(t *testing.T) {
		testZMapPutStr(t, keys)
	})
	t.Run("get", func(t *testing.T) {
		testZMapGetStr(t, keys)
	})
}

func testZMapPutStr(t *testing.T, keys []string) {
	m := zend.NewZMap(uint32(len(keys)))
	assert.Equal(t, 0, m.Count())
	for i, key := range keys {
		m.Put(zend.TZvalStr(key), zend.TZvalLong(int64(i)))
	}
	assert.Equal(t, len(keys), m.Count())
	// overwrite
	for i, key := range keys {
		m.Put(zend.TZvalStr(key), zend.TZvalLong(int64(-i)))
	}
	assert.Equal(t, len(keys), m.Count())
	for i, key := range keys {
		act, ok := m.Get(zend.TZvalStr(key))
		assert.True(t, ok)
		assert.Equal(t, int64(-i), act.TZLong_())
	}
	assert.Equal(t, len(keys), int(m.GetResident()))
}

func testZMapGetStr(t *testing.T, keys []string) {
	m := zend.NewZMap(uint32(len(keys)))
	for i, key := range keys {
		m.Put(zend.TZvalStr(key), zend.TZvalLong(int64(i)))
	}
	for i, key := range keys {
		act, ok := m.Get(zend.TZvalStr(key))
		assert.True(t, ok)
		assert.Equal(t, int64(i), act.TZLong_())
	}
}

//endregion

func BenchmarkStringMaps8(b *testing.B) {
	const keySz = 16
	sizes := []int{128, 1024 * 8, 1024 * 64, 1024 * 512, 1024 * 1024 * 4}
	for _, n := range sizes {
		data := genStringData8(keySz, n)
		tdata := zend.Strs2TZvals(data)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkRuntimeMap8(b, data)
			})

			b.Run("swiss.Map", func(b *testing.B) {
				benchmarkSwissMap(b, data)
			})

			b.Run("swiss.Map8", func(b *testing.B) {
				benchmarkSwissMap8(b, data)
			})

			b.Run("minswiss.SwissMap", func(b *testing.B) {
				benchmarkSwissMinMap(b, data)
			})

			b.Run("zend.ZMap", func(b *testing.B) {
				benchmarkZMap(b, tdata)
			})
		})
	}
}

func BenchmarkInt64Maps8(b *testing.B) {
	sizes := []int{16, 128, 1024, 1024 * 8, 1024 * 64, 1024 * 512, 1024 * 1024 * 4}
	for _, n := range sizes {
		data := generateInt64Data8(n)
		tdata := zend.Ints2TZvals(data)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkRuntimeMap8(b, data)
			})
			b.Run("swiss.Map", func(b *testing.B) {
				benchmarkSwissMap(b, data)
			})
			b.Run("swiss.Map8", func(b *testing.B) {
				benchmarkSwissMap8(b, data)
			})

			b.Run("minswiss.SwissMap", func(b *testing.B) {
				benchmarkSwissMinMap(b, data)
			})

			b.Run("zend.ZMap", func(b *testing.B) {
				benchmarkZMap(b, tdata)
			})
		})
	}
}

func TestMemoryFootprint8(t *testing.T) {
	t.Skip("unskip for memory footprint stats")
	var samples []float64
	for n := 10; n <= 10_000; n += 10 {
		b1 := testing.Benchmark(func(b *testing.B) {
			// max load factor 7/8
			m := NewMap8[int, int](uint32(n))
			require.NotNil(b, m)
		})
		b2 := testing.Benchmark(func(b *testing.B) {
			// max load factor 6.5/8
			m := make(map[int]int, n)
			require.NotNil(b, m)
		})
		x := float64(b1.MemBytes) / float64(b2.MemBytes)
		samples = append(samples, x)
	}
	t.Logf("mean size ratio: %.3f", mean8(samples))
}

func benchmarkRuntimeMap8[K comparable](b *testing.B, keys []K) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := make(map[K]K, n)
	for _, k := range keys {
		m[k] = k
	}
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m[keys[uint32(i*17)&mod]]
	}
	assert.True(b, ok)
	b.ReportAllocs()
}

func benchmarkSwissMap8[K comparable](b *testing.B, keys []K) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := NewMap8[K, K](n)
	for _, k := range keys {
		m.Put(k, k)
	}
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m.Get(keys[uint32(i*17)&mod])
	}
	assert.True(b, ok)
	b.ReportAllocs()
}

func benchmarkSwissMinMap[K comparable](b *testing.B, keys []K) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := minswiss.NewSwissMap[K, K](n)
	for _, k := range keys {
		m.Put(k, k)
	}
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m.Get(keys[uint32(i*17)&mod])
	}
	assert.True(b, ok)
	b.ReportAllocs()
}
func benchmarkZMap(b *testing.B, keys []*zend.TZval) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := zend.NewZMap(n)
	for _, k := range keys {
		m.Put(k, k)
	}
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		_, ok = m.Get(keys[uint32(i*17)&mod])
	}
	assert.True(b, ok)
	b.ReportAllocs()
}

func generateInt64Data8(n int) (data []int64) {
	data = make([]int64, n)
	var x int64
	for i := range data {
		x += rand.Int63n(128) + 1
		data[i] = x
	}
	return
}

func mean8(samples []float64) (m float64) {
	for _, s := range samples {
		m += s
	}
	return m / float64(len(samples))
}

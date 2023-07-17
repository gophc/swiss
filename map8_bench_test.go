package swiss

import (
	"github.com/dolthub/swiss/zend"
	"github.com/stretchr/testify/require"
	"math/bits"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkStringMaps8(b *testing.B) {
	const keySz = 8
	sizes := []int{16, 128, 1024, 1024 * 8, 1024 * 64, 1024 * 512, 1024 * 1024 * 4}
	for _, n := range sizes {
		data := genStringData8(keySz, n)
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

			b.Run("zend.SwissMap", func(b *testing.B) {
				benchmarkSwissZMap(b, data)
			})
		})
	}
}

func TestInt64MapsProf(t *testing.T) {
	const N = 10000
	sizes := [...]int{16, 128, 1024, 1024 * 8, 1024 * 64, 1024 * 512, 1024 * 1024 * 4}
	nn := sizes[5]
	keys := generateInt64Data8(nn)

	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(t, 1, bits.OnesCount32(n))
	m := zend.NewSwissMap[int64, int64](n)
	for _, k := range keys {
		m.Put(k, k)
	}
	var ok bool
	for i := 0; i < N; i++ {
		_, ok = m.Get(keys[uint32(i*17)&mod])
	}
	assert.True(t, ok)

}

func BenchmarkInt64Maps8(b *testing.B) {
	sizes := []int{16, 128, 1024, 1024 * 8, 1024 * 64, 1024 * 512, 1024 * 1024 * 4}
	for _, n := range sizes {
		data := generateInt64Data8(n)
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

			b.Run("zend.SwissMap", func(b *testing.B) {
				benchmarkSwissZMap(b, data)
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

func benchmarkSwissZMap[K comparable](b *testing.B, keys []K) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := zend.NewSwissMap[K, K](n)
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

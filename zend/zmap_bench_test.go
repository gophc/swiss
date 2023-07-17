package zend

import (
	"math/bits"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func BenchmarkStrSwissMaps(b *testing.B) {
	const keySz = 8
	sizes := []int{16, 128, 1024, 1024 * 8, 1024 * 64, 1024 * 512, 1024 * 1024}
	for _, n := range sizes {
		data := genSwissStringData(keySz, n)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkSwissStdMap(b, data)
			})
			b.Run("swiss.SwissMap", func(b *testing.B) {
				benchmarkSwissMap(b, data)
			})
		})
	}
}

func BenchmarkInt64SwissMaps(b *testing.B) {
	sizes := []int{16, 128, 1024, 1024 * 8, 1024 * 64, 1024 * 512, 1024 * 1024 * 4}
	for _, n := range sizes {
		data := generateSwissInt64Data(n)
		b.Run("n="+strconv.Itoa(n), func(b *testing.B) {
			b.Run("runtime map", func(b *testing.B) {
				benchmarkSwissStdMap(b, data)
			})
			b.Run("swiss.SwissMap", func(b *testing.B) {
				benchmarkSwissMap(b, data)
			})
		})
	}
}

func TestMemorySwissFootprint(t *testing.T) {
	t.Skip("unskip for memory footprint stats")
	var samples []float64
	for n := 10; n <= 10_000; n += 10 {
		b1 := testing.Benchmark(func(b *testing.B) {
			// max load factor 7/8
			m := NewSwissMap[int, int](uint32(n))
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
	t.Logf("mean size ratio: %.3f", swissMean(samples))
}

func TestMemorySwissMapDbg(t *testing.T) {
	keys := genSwissStringData(16, 34_000)
	t.Run("iter", func(t *testing.T) {
		testSwissMapIter(t, keys)
	})
}

func benchmarkSwissStdMap[K comparable](b *testing.B, keys []K) {
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

func benchmarkSwissMap[K comparable](b *testing.B, keys []K) {
	n := uint32(len(keys))
	mod := n - 1 // power of 2 fast modulus
	require.Equal(b, 1, bits.OnesCount32(n))
	m := NewSwissMap[K, K](n)
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

func generateSwissInt64Data(n int) (data []int64) {
	data = make([]int64, n)
	var x int64
	for i := range data {
		x += rand.Int63n(128) + 1
		data[i] = x
	}
	return
}

func swissMean(samples []float64) (m float64) {
	for _, s := range samples {
		m += s
	}
	return m / float64(len(samples))
}

package zend

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func FuzzStrZMap(f *testing.F) {
	f.Add(uint8(1), 14, 50)
	f.Add(uint8(2), 1, 1)
	f.Add(uint8(2), 14, 14)
	f.Add(uint8(2), 14, 15)
	f.Add(uint8(2), 25, 100)
	f.Add(uint8(2), 25, 1000)
	f.Add(uint8(8), 0, 1)
	f.Add(uint8(8), 1, 1)
	f.Add(uint8(8), 14, 14)
	f.Add(uint8(8), 14, 15)
	f.Add(uint8(8), 25, 100)
	f.Add(uint8(8), 25, 1000)
	f.Fuzz(func(t *testing.T, keySz uint8, init, count int) {
		// smaller key sizes generate more overwrites
		fuzzSwissTestStrZMap(t, uint32(keySz), uint32(init), uint32(count))
	})
}

func fuzzSwissTestStrZMap(t *testing.T, keySz, init, count uint32) {
	const limit = 1024 * 1024
	if count > limit || init > limit {
		t.Skip()
	}
	m := NewSwissMap[string, int](init)
	if count == 0 {
		return
	}
	// make tests deterministic
	swissSetConstSeed(m, 1)

	keys := genSwissStringData(int(keySz), int(count))
	golden := make(map[string]int, init)
	for i, k := range keys {
		m.Put(k, i)
		golden[k] = i
	}
	assert.Equal(t, len(golden), m.Count())

	for k, exp := range golden {
		act, ok := m.Get(k)
		assert.True(t, ok)
		assert.Equal(t, exp, act)
	}
	for _, k := range keys {
		_, ok := golden[k]
		assert.True(t, ok)
		assert.True(t, m.Has(k))
	}

	deletes := keys[:count/2]
	for _, k := range deletes {
		delete(golden, k)
		m.Delete(k)
	}
	assert.Equal(t, len(golden), m.Count())

	for _, k := range deletes {
		assert.False(t, m.Has(k))
	}
	for k, exp := range golden {
		act, ok := m.Get(k)
		assert.True(t, ok)
		assert.Equal(t, exp, act)
	}
}

type swissHasher struct {
	hash func()
	seed uintptr
}

func swissSetConstSeed[K comparable, V any](m *SwissMap[K, V], seed uintptr) {
	h := (*swissHasher)((unsafe.Pointer)(&m.hash))
	h.seed = seed
}

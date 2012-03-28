package diskv

import (
	"fmt"
	"math/rand"
	"testing"
)

func shuffle(keys []Key) {
	ints := rand.Perm(len(keys))
	for i, _ := range keys {
		keys[i], keys[ints[i]] = keys[ints[i]], keys[i]
	}
}

func gen_value(size int) Value {
	v := make([]byte, size)
	for i := 0; i < size; i++ {
		v[i] = uint8((rand.Int() % 26) + 97) // a-z
	}
	return v
}

const (
	KEY_COUNT = 1000
)

func gen_keys() []Key {
	keys := make([]Key, KEY_COUNT)
	for i := 0; i < KEY_COUNT; i++ {
		keys[i] = Key(fmt.Sprintf("%d", i))
	}
	return keys
}

func (s *DStore) load(keys []Key, v Value) {
	for _, k := range keys {
		s.Write(k, v)
	}
}

func bench_read(b *testing.B, size, cachesz int) {
	b.StopTimer()
	s := NewDStore(".", dumb_xf, uint(cachesz))
	defer s.Flush()
	keys := gen_keys()
	value := gen_value(size)
	s.load(keys, value)
	shuffle(keys)
	b.SetBytes(int64(size))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _ = s.Read(keys[i%len(keys)])
	}
	b.StopTimer()
}

func keyless(a, b interface{}) bool {
	return a.(Key) < b.(Key)
}

func bench_write(b *testing.B, size int, with_index bool) {
	b.StopTimer()
	var s Store
	if with_index {
		s = NewODStore(".", dumb_xf, 0, keyless)
	} else {
		s = NewDStore(".", dumb_xf, 0)
	}
	defer s.Flush()
	keys := gen_keys()
	value := gen_value(size)
	shuffle(keys)
	b.SetBytes(int64(size))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Write(keys[i%len(keys)], value)
	}
	b.StopTimer()
}

func BenchmarkWrite_1K_NoIndex(b *testing.B) {
	bench_write(b, 1024, false)
}

func BenchmarkWrite_4K_NoIndex(b *testing.B) {
	bench_write(b, 4096, false)
}

func BenchmarkWrite_10K_NoIndex(b *testing.B) {
	bench_write(b, 10240, false)
}

func BenchmarkWrite_1K_WithIndex(b *testing.B) {
	bench_write(b, 1024, true)
}

func BenchmarkWrite_4K_WithIndex(b *testing.B) {
	bench_write(b, 4096, true)
}

func BenchmarkWrite_10K_WithIndex(b *testing.B) {
	bench_write(b, 10240, true)
}

func BenchmarkRead_1K_NoCache(b *testing.B) {
	bench_read(b, 1024, 0)
}

func BenchmarkRead_1K_WithCache(b *testing.B) {
	bench_read(b, 1024, KEY_COUNT*1024*2)
}

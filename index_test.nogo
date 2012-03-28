package diskv

import (
	"testing"
	"bytes"
	"time"
)

func simple_xf(k Key) []string {
	return []string{string(k)}
}

func strless(a, b interface{}) bool {
	return a.(Key) < b.(Key)
}

func cmp(a, b []Key) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIndexOrder(t *testing.T) {
	s := NewODStore(".", simple_xf, 1024, strless)
	defer s.Flush()
	v := Value([]byte{'1', '2', '3'})
	_ = s.Write(Key("a"), v)
	_ = s.Write(Key("1"), v)
	_ = s.Write(Key("m"), v)
	_ = s.Write(Key("-"), v)
	_ = s.Write(Key("A"), v)
	expected_keys := []Key{"-", "1", "A", "a", "m"}
	if keys, err := s.KeysFrom("", 100); err != nil {
		t.Fatalf("%s", err)
	} else if len(keys) != 5 {
		t.Fatalf("KeysFrom: got %d, expected %d", len(keys), 5)
	} else if !cmp(keys, expected_keys) {
		t.Fatalf("KeysFrom: got %s, expected %s", keys, expected_keys)
	}
}

func TestIndexLoad(t *testing.T) {
	s := NewODStore(".", simple_xf, 1024, strless)
	defer s.Flush()
	v := Value([]byte{'1', '2', '3'})
	keys := []Key{"a", "b", "c", "d", "e", "f", "g"}
	for _, k := range keys {
		_ = s.Write(k, v)
	}
	s2 := NewODStore(".", simple_xf, 1024, strless)
	for _, k := range keys {
		if !s2.IsIndexed(k) {
			t.Fatalf("key %s not indexed", k)
		}
	}
	// cache one
	if read_v, err := s2.Read(keys[0]); err != nil {
		t.Fatalf("%s", err)
	} else if bytes.Compare(v, read_v) != 0 {
		t.Fatalf("%s: got %s, expected %s", keys[0], read_v, v)
	}
	for i := 0; i < 10 && !s2.IsCached(keys[0]); i++ {
		time.Sleep(10e6) // 10 ms
	}
	if !s2.IsCached(keys[0]) {
		t.Fatalf("key %s not cached", keys[0])
	}
	// kill the disk
	s.Flush()
	// should still be there
	if read_v2, err := s2.Read(keys[0]); err != nil {
		t.Fatalf("%s", err)
	} else if bytes.Compare(v, read_v2) != 0 {
		t.Fatalf("%s: got %s, expected %s", keys[0], read_v2, v)
	}
	// but not in the original
	if _, err := s.Read(keys[0]); err == nil {
		t.Fatalf("expected error reading from flushed store")
	}
}

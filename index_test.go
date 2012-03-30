package diskv

import (
	"bytes"
	"testing"
	"time"
)

func simpleXf(k string) []string {
	return []string{string(k)}
}

func strLess(a, b interface{}) bool {
	return a.(string) < b.(string)
}

func cmp(a, b []string) bool {
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
	s := NewOrderedStore("index-test", simpleXf, 1024)
	s.ResetOrder(strLess)
	defer s.Flush()
	v := []byte{'1', '2', '3'}
	_ = s.Write("a", v)
	_ = s.Write("1", v)
	_ = s.Write("m", v)
	_ = s.Write("-", v)
	_ = s.Write("A", v)
	expectedKeys := []string{"-", "1", "A", "a", "m"}
	if keys, err := s.KeysFrom("", 100); err != nil {
		t.Fatalf("%s", err)
	} else if len(keys) != 5 {
		t.Fatalf("KeysFrom: got %d, expected %d", len(keys), 5)
	} else if !cmp(keys, expectedKeys) {
		t.Fatalf("KeysFrom: got %s, expected %s", keys, expectedKeys)
	}
}

func TestIndexLoad(t *testing.T) {
	s := NewOrderedStore("index-test", simpleXf, 1024)
	s.ResetOrder(strLess)
	defer s.Flush()
	v := []byte{'1', '2', '3'}
	keys := []string{"a", "b", "c", "d", "e", "f", "g"}
	for _, k := range keys {
		_ = s.Write(k, v)
	}
	s2 := NewOrderedStore("index-test", simpleXf, 1024)
	s2.ResetOrder(strLess)
	for _, k := range keys {
		if !s2.IsIndexed(k) {
			t.Fatalf("key %s not indexed", k)
		}
	}
	// cache one
	if readValue, err := s2.Read(keys[0]); err != nil {
		t.Fatalf("%s", err)
	} else if bytes.Compare(v, readValue) != 0 {
		t.Fatalf("%s: got %s, expected %s", keys[0], readValue, v)
	}
	for i := 0; i < 10 && !s2.IsCached(keys[0]); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !s2.IsCached(keys[0]) {
		t.Fatalf("key %s not cached", keys[0])
	}
	// kill the disk
	s.Flush()
	// should still be there
	if readValue2, err := s2.Read(keys[0]); err != nil {
		t.Fatalf("%s", err)
	} else if bytes.Compare(v, readValue2) != 0 {
		t.Fatalf("%s: got %s, expected %s", keys[0], readValue2, v)
	}
	// but not in the original
	if _, err := s.Read(keys[0]); err == nil {
		t.Fatalf("expected error reading from flushed store")
	}
}

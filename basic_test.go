package diskv

import (
	"bytes"
	"testing"
	"time"
)

func dumbXf(k string) []string {
	return []string{}
}

func TestWriteReadErase(t *testing.T) {
	s := NewStore("test-data", dumbXf, 1024)
	defer s.Flush()
	k, v := "a", []byte{'b'}
	if err := s.Write(k, v); err != nil {
		t.Fatalf("write: %s", err)
	}
	if read_v, err := s.Read(k); err != nil {
		t.Fatalf("read: %s", err)
	} else if bytes.Compare(v, read_v) != 0 {
		t.Fatalf("read: expected %s, got %s", v, read_v)
	}
	if err := s.Erase(k); err != nil {
		t.Fatalf("erase: %s", err)
	}
}

func TestWRECache(t *testing.T) {
	s := NewStore("test-data", dumbXf, 1024)
	defer s.Flush()
	k, v := "xxx", []byte{' ', ' ', ' '}
	if s.IsCached(k) {
		t.Fatalf("key cached before Write and Read")
	}
	if err := s.Write(k, v); err != nil {
		t.Fatalf("write: %s", err)
	}
	if s.IsCached(k) {
		t.Fatalf("key cached before Read")
	}
	if read_v, err := s.Read(k); err != nil {
		t.Fatalf("read: %s", err)
	} else if bytes.Compare(v, read_v) != 0 {
		t.Fatalf("read: expected %s, got %s", v, read_v)
	}
	for i := 0; i < 10 && !s.IsCached(k); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !s.IsCached(k) {
		t.Fatalf("key not cached after Read")
	}
	if err := s.Erase(k); err != nil {
		t.Fatalf("erase: %s", err)
	}
	if s.IsCached(k) {
		t.Fatalf("key cached after Erase")
	}
}

func Teststrings(t *testing.T) {
	s := NewStore("test-data", dumbXf, 1024)
	defer s.Flush()
	keys := map[string]bool{"a": false, "b": false, "c": false, "d": false}
	v := []byte{'1'}
	for k, _ := range keys {
		if err := s.Write(k, v); err != nil {
			t.Fatalf("write: %s: %s", k, err)
		}
	}
	c := s.Keys()
	for k := <-c; len(k) > 0; k = <-c { // valid key must be len > 0
		if _, present := keys[k]; present {
			keys[k] = true
			t.Logf("got: %s\n", k)
		} else {
			t.Fatalf("strings() returns unknown key: %s", k)
		}
	}
	for k, found := range keys {
		if !found {
			t.Fatalf("never got %s", k)
		}
	}
}

func TestZeroByteCache(t *testing.T) {
	s := NewStore("test-data", dumbXf, 0)
	defer s.Flush()
	k, v := "a", []byte{'1', '2', '3'}
	if err := s.Write(k, v); err != nil {
		t.Fatalf("Write: %s", err)
	}
	if s.IsCached(k) {
		t.Fatalf("key cached, expected not-cached")
	}
	if _, err := s.Read(k); err != nil {
		t.Fatalf("Read: %s", err)
	}
	if s.IsCached(k) {
		t.Fatalf("key cached, expected not-cached")
	}
}

func TestOneByteCache(t *testing.T) {
	s := NewStore("test-data", dumbXf, 1)
	defer s.Flush()
	k1, k2, v1, v2 := "a", "b", []byte{'1'}, []byte{'1', '2'}
	if err := s.Write(k1, v1); err != nil {
		t.Fatalf("%s", err)
	}
	if _, err := s.Read(k1); err != nil {
		t.Fatalf("%s", err)
	}
	for i := 0; i < 10 && !s.IsCached(k1); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !s.IsCached(k1) {
		t.Fatalf("expected 1-byte value to be cached, but it wasn't")
	}
	if err := s.Write(k2, v2); err != nil {
		t.Fatalf("%s", err)
	}
	if _, err := s.Read(k2); err != nil {
		t.Fatalf("%s", err)
	}
	for i := 0; i < 10 && (!s.IsCached(k1) || s.IsCached(k2)); i++ {
		time.Sleep(10 * time.Millisecond) // just wait for lazy-cache
	}
	if !s.IsCached(k1) {
		t.Fatalf("1-byte value was uncached for no reason")
	}
	if s.IsCached(k2) {
		t.Fatalf("2-byte value was cached, but cache max size is 1")
	}
}

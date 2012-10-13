package diskv

import (
	"bytes"
	"testing"
	"time"
)

func dumbXf(k string) []string {
	return []string{}
}

func (d *Diskv) isCached(key string) bool {
	d.RLock()
	defer d.RUnlock()
	_, ok := d.cache[key]
	return ok
}

func TestWriteReadErase(t *testing.T) {
	d := New(&Options{BasePath: "test-data", Transform: dumbXf, CacheSizeMax: 1024})
	defer d.Flush()
	k, v := "a", []byte{'b'}
	if err := d.Write(k, v); err != nil {
		t.Fatalf("write: %s", err)
	}
	if read_v, err := d.Read(k); err != nil {
		t.Fatalf("read: %s", err)
	} else if bytes.Compare(v, read_v) != 0 {
		t.Fatalf("read: expected %s, got %s", v, read_v)
	}
	if err := d.Erase(k); err != nil {
		t.Fatalf("erase: %s", err)
	}
}

func TestWRECache(t *testing.T) {
	d := New(&Options{BasePath: "test-data", Transform: dumbXf, CacheSizeMax: 1024})
	defer d.Flush()
	k, v := "xxx", []byte{' ', ' ', ' '}
	if d.isCached(k) {
		t.Fatalf("key cached before Write and Read")
	}
	if err := d.Write(k, v); err != nil {
		t.Fatalf("write: %s", err)
	}
	if d.isCached(k) {
		t.Fatalf("key cached before Read")
	}
	if read_v, err := d.Read(k); err != nil {
		t.Fatalf("read: %s", err)
	} else if bytes.Compare(v, read_v) != 0 {
		t.Fatalf("read: expected %s, got %s", v, read_v)
	}
	for i := 0; i < 10 && !d.isCached(k); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !d.isCached(k) {
		t.Fatalf("key not cached after Read")
	}
	if err := d.Erase(k); err != nil {
		t.Fatalf("erase: %s", err)
	}
	if d.isCached(k) {
		t.Fatalf("key cached after Erase")
	}
}

func TestStrings(t *testing.T) {
	d := New(&Options{BasePath: "test-data", Transform: dumbXf, CacheSizeMax: 1024})
	//defer d.Flush()

	keys := map[string]bool{"a": false, "b": false, "c": false, "d": false}
	v := []byte{'1'}
	for k, _ := range keys {
		if err := d.Write(k, v); err != nil {
			t.Fatalf("write: %s: %s", k, err)
		}
	}

	for k := range d.Keys() {
		if _, present := keys[k]; present {
			keys[k] = true
			t.Logf("got: %s\n", k)
		} else {
			t.Fatalf("strings() returns unknown key: %s", k)
		}
	}

	for k, found := range keys {
		if !found {
			t.Errorf("never got %s", k)
		}
	}
}

func TestZeroByteCache(t *testing.T) {
	d := New(&Options{BasePath: "test-data", Transform: dumbXf, CacheSizeMax: 0})
	defer d.Flush()
	k, v := "a", []byte{'1', '2', '3'}
	if err := d.Write(k, v); err != nil {
		t.Fatalf("Write: %s", err)
	}
	if d.isCached(k) {
		t.Fatalf("key cached, expected not-cached")
	}
	if _, err := d.Read(k); err != nil {
		t.Fatalf("Read: %s", err)
	}
	if d.isCached(k) {
		t.Fatalf("key cached, expected not-cached")
	}
}

func TestOneByteCache(t *testing.T) {
	d := New(&Options{BasePath: "test-data", Transform: dumbXf, CacheSizeMax: 1})
	defer d.Flush()
	k1, k2, v1, v2 := "a", "b", []byte{'1'}, []byte{'1', '2'}
	if err := d.Write(k1, v1); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Read(k1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10 && !d.isCached(k1); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !d.isCached(k1) {
		t.Fatalf("expected 1-byte value to be cached, but it wasn't")
	}
	if err := d.Write(k2, v2); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Read(k2); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10 && (!d.isCached(k1) || d.isCached(k2)); i++ {
		time.Sleep(10 * time.Millisecond) // just wait for lazy-cache
	}
	if !d.isCached(k1) {
		t.Fatalf("1-byte value was uncached for no reason")
	}
	if d.isCached(k2) {
		t.Fatalf("2-byte value was cached, but cache max size is 1")
	}
}

func TestStaleCache(t *testing.T) {
	d := New(&Options{BasePath: "test-data", Transform: dumbXf, CacheSizeMax: 1})
	defer d.Flush()
	k, first, second := "a", "first", "second"
	if err := d.Write(k, []byte(first)); err != nil {
		t.Fatal(err)
	}
	v, err := d.Read(k)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != first {
		t.Errorf("expected '%s', got '%s'", first, v)
	}
	if err := d.Write(k, []byte(second)); err != nil {
		t.Fatal(err)
	}
	v, err = d.Read(k)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != second {
		t.Errorf("expected '%s', got '%s'", second, v)
	}
}

package diskv

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// ReadStream from cache shouldn't panic on a nil dereference from a nonexistent
// Compression :)
func TestIssue2A(t *testing.T) {
	d := New(Options{
		BasePath:     "test-issue-2a",
		CacheSizeMax: 1024,
	})
	defer d.EraseAll()

	input := "abcdefghijklmnopqrstuvwxy"
	key, writeBuf, sync := "a", bytes.NewBufferString(input), false
	if err := d.WriteStream(key, writeBuf, sync); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		began := time.Now()
		rc, err := d.ReadStream(key, false)
		if err != nil {
			t.Fatal(err)
		}
		buf, err := ioutil.ReadAll(rc)
		if err != nil {
			t.Fatal(err)
		}
		if !cmpBytes(buf, []byte(input)) {
			t.Fatalf("read #%d: '%s' != '%s'", i+1, string(buf), input)
		}
		rc.Close()
		t.Logf("read #%d in %s", i+1, time.Since(began))
	}
}

// ReadStream on a key that resolves to a directory should return an error.
func TestIssue2B(t *testing.T) {
	blockTransform := func(s string) []string {
		transformBlockSize := 3
		sliceSize := len(s) / transformBlockSize
		pathSlice := make([]string, sliceSize)
		for i := 0; i < sliceSize; i++ {
			from, to := i*transformBlockSize, (i*transformBlockSize)+transformBlockSize
			pathSlice[i] = s[from:to]
		}
		return pathSlice
	}

	d := New(Options{
		BasePath:     "test-issue-2b",
		Transform:    blockTransform,
		CacheSizeMax: 0,
	})
	defer d.EraseAll()

	v := []byte{'1', '2', '3'}
	if err := d.Write("abcabc", v); err != nil {
		t.Fatal(err)
	}

	_, err := d.ReadStream("abc", false)
	if err == nil {
		t.Fatal("ReadStream('abc') should return error")
	}
	t.Logf("ReadStream('abc') returned error: %v", err)
}

// Ensure ReadStream with direct=true isn't racy.
func TestIssue17(t *testing.T) {
	var (
		basePath = "test-data"
	)

	dWrite := New(Options{
		BasePath:     basePath,
		CacheSizeMax: 0,
	})
	defer dWrite.EraseAll()

	dRead := New(Options{
		BasePath:     basePath,
		CacheSizeMax: 50,
	})

	cases := map[string]string{
		"a": `1234567890`,
		"b": `2345678901`,
		"c": `3456789012`,
		"d": `4567890123`,
		"e": `5678901234`,
	}

	for k, v := range cases {
		if err := dWrite.Write(k, []byte(v)); err != nil {
			t.Fatalf("during write: %s", err)
		}
		dRead.Read(k) // ensure it's added to cache
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for k, v := range cases {
		wg.Add(1)
		go func(k, v string) {
			<-start
			dRead.ReadStream(k, true)
			wg.Done()
		}(k, v)
	}
	close(start)
	wg.Wait()
}

// Test for issue #40, where acquiring two stream readers on the same k/v pair
// caused the value to be written into the cache twice, messing up the
// size calculations.
func TestIssue40(t *testing.T) {
	var (
		basePath = "test-data"
	)
	// Simplest transform function: put all the data files into the base dir.
	flatTransform := func(s string) []string { return []string{} }

	// Initialize a new diskv store, rooted at "my-data-dir",
	// with a 100 byte cache.
	d := New(Options{
		BasePath:     basePath,
		Transform:    flatTransform,
		CacheSizeMax: 100,
	})

	defer d.EraseAll()

	// Write a 50 byte value, filling the cache half-way
	k1 := "key1"
	d1 := make([]byte, 50)
	rand.Read(d1)
	d.Write(k1, d1)

	// Get *two* read streams on it. Because the key is not yet in the cache,
	// and will not be in the cache until a stream is fully read, both
	// readers use the 'siphon' object, which always writes to the cache
	// after reading.
	s1, err := d.ReadStream(k1, false)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := d.ReadStream(k1, false)
	if err != nil {
		t.Fatal(err)
	}
	// When each stream is drained, the underlying siphon will write
	// the value into the cache's map and increment the cache size.
	// This means we will have 1 entry in the cache map
	// ("key1" mapping to a 50 byte slice) but the cache size will be 100,
	// because the buggy code does not check if an entry already exists
	// in the map.
	// s1 drains:
	//   cache[k] = v
	//   cacheSize += len(v)
	// s2 drains:
	//   cache[k] = v /* overwrites existing */
	//   cacheSize += len(v) /* blindly adds to the cache size */
	ioutil.ReadAll(s1)
	ioutil.ReadAll(s2)

	// Now write a different k/v pair, with a 60 byte array.
	k2 := "key2"
	d2 := make([]byte, 60)
	rand.Read(d2)
	d.Write(k2, d2)
	// The act of reading the k/v pair back out causes it to be cached.
	// Because the cache is only 100 bytes, it needs to delete existing
	// entries to make room.
	// If the cache is buggy, it will delete the single 50-byte entry
	// from the cache map & decrement cacheSize by 50... but because
	// cacheSize was improperly incremented twice earlier, this will
	// leave us with no entries in the cacheMap but with cacheSize==50.
	// Since CacheSizeMax-cacheSize (100-50) is less than 60, there
	// is no room in the cache for this entry and it panics.
	d.Read(k2)
}

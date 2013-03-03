package diskv

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"
)

func TestBasicStreamCaching(t *testing.T) {
	d := New(Options{
		BasePath:     "test-data",
		Transform:    func(string) []string { return []string{} },
		CacheSizeMax: 1024,
	})
	defer d.EraseAll()

	input := "a1b2c3"
	key, writeBuf, sync := "a", bytes.NewBufferString(input), true
	if err := d.WriteStream(key, writeBuf, sync); err != nil {
		t.Fatal(err)
	}

	if d.isCached(key) {
		t.Fatalf("'%s' cached, but shouldn't be (yet)")
	}

	rc, err := d.ReadStream(key)
	if err != nil {
		t.Fatal(err)
	}

	readBuf, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}

	if !cmpBytes(readBuf, []byte(input)) {
		t.Fatalf("'%s' != '%s'", string(readBuf), input)
	}

	if !d.isCached(key) {
		t.Fatalf("'%s' isn't cached, but should be")
	}
}

func TestTwoReads(t *testing.T) {
	d := New(Options{
		BasePath:     "test-two-reads",
		Transform:    func(string) []string { return []string{} },
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
		rc, err := d.ReadStream(key)
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

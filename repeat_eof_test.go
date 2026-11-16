package diskv

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestReadStreamRepeatedEOF(t *testing.T) {
	for _, cacheSize := range []uint64{0, 1024} {
		cacheName := "uncached"
		if cacheSize > 0 {
			cacheName = "cache-enabled"
		}
		t.Run(cacheName, func(t *testing.T) {
			for _, tc := range []struct {
				name        string
				compression Compression
			}{
				{"plain", nil},
				{"gzip", NewGzipCompression()},
				{"zlib", NewZlibCompression()},
				{"zlib-dict", NewZlibCompressionLevelDict(-1, []byte("stored value"))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					dir, err := ioutil.TempDir("", "diskv-repeat-eof-")
					if err != nil {
						t.Fatal(err)
					}
					defer os.RemoveAll(dir)
					d := New(Options{BasePath: dir, CacheSizeMax: cacheSize, Compression: tc.compression})
					value := []byte("stored value")
					if err := d.Write("key", value); err != nil {
						t.Fatal(err)
					}
					r, err := d.ReadStream("key", false)
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					got, err := ioutil.ReadAll(r)
					if err != nil || !bytes.Equal(got, value) {
						t.Fatalf("ReadAll = %q, %v", got, err)
					}
					for i := 0; i < 2; i++ {
						n, err := r.Read(make([]byte, 1))
						if n != 0 {
							t.Errorf("read after EOF returned %d bytes", n)
						}
						if tc.compression == nil {
							// Plain disk streams already report the file's close after EOF.
							pe, ok := err.(*os.PathError)
							if !ok || pe.Err != os.ErrClosed {
								t.Errorf("plain read after EOF = %v, want closed-file error", err)
							}
						} else if err != io.EOF {
							t.Errorf("compressed read after EOF = %v, want EOF", err)
						}
					}
				})
			}
		})
	}
}

// Closing this decoder invalidates its read state, like zlib's Close does.
type eofCloseSpy struct {
	reads, closes int
	closeErr      error
}

func (r *eofCloseSpy) Read(p []byte) (int, error) {
	r.reads++
	if r.closes > 0 {
		return 0, os.ErrClosed
	}
	return copy(p, "x"), io.EOF
}

func (r *eofCloseSpy) Close() error {
	r.closes++
	return r.closeErr
}

func TestClosingReaderPreservesEOF(t *testing.T) {
	decoderErr := errors.New("decoder close")
	sourceErr := errors.New("source close")
	for _, tc := range []struct {
		name                        string
		decoderErr, sourceErr, want error
	}{
		{"success", nil, nil, io.EOF},
		{"decoder-error", decoderErr, nil, decoderErr},
		{"source-error", nil, sourceErr, sourceErr},
		{"both-errors", decoderErr, sourceErr, decoderErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			decoder := &eofCloseSpy{closeErr: tc.decoderErr}
			sourceCloses := 0
			r := &closingReader{
				rc:          decoder,
				source:      &closeSpy{closes: &sourceCloses, closeErr: tc.sourceErr},
				preserveEOF: true,
			}
			buf := make([]byte, 1)
			if n, err := r.Read(buf); n != 1 || buf[0] != 'x' || err != tc.want {
				t.Fatalf("first read = %d, %q, %v; want 1, x, %v", n, buf, err, tc.want)
			}
			for i := 0; i < 2; i++ {
				if n, err := r.Read(buf); n != 0 || err != io.EOF {
					t.Errorf("read after EOF = %d, %v", n, err)
				}
			}
			if decoder.reads != 1 || decoder.closes != 1 || sourceCloses != 1 {
				t.Errorf("decoder reads/closes = %d/%d, source closes = %d; want 1 each", decoder.reads, decoder.closes, sourceCloses)
			}
		})
	}
}

package diskv

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// Retain the source reader so that an os.File finalizer cannot hide a leak.
type captureCompression struct {
	Compression
	source        io.Reader
	initErr       error
	closeErr      error
	decoderCloses int
}

func (c *captureCompression) Reader(r io.Reader) (io.ReadCloser, error) {
	c.source = r
	if c.initErr != nil {
		return nil, c.initErr
	}
	if c.closeErr != nil {
		return &closeSpy{Reader: r, closeErr: c.closeErr, closes: &c.decoderCloses}, nil
	}
	return c.Compression.Reader(r)
}

type closeSpy struct {
	io.Reader
	closeErr error
	closes   *int
}

func (r *closeSpy) Close() error {
	*r.closes++
	return r.closeErr
}

func assertSourceClosed(t *testing.T, source io.Reader) {
	t.Helper()
	// A zero-length read does not consume data or induce EOF on an open file.
	// The retained reader must report the closed file instead.
	_, err := source.Read(nil)
	pe, ok := err.(*os.PathError)
	if !ok || pe.Err != os.ErrClosed {
		t.Errorf("underlying file remains open: Read(nil) returned %v", err)
	}
}

func TestReadStreamClosesCompressedFile(t *testing.T) {
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
				{"gzip", NewGzipCompression()},
				{"zlib", NewZlibCompression()},
				{"zlib-dict", NewZlibCompressionLevelDict(-1, []byte("stored value"))},
			} {
				t.Run(tc.name, func(t *testing.T) {
					for _, mode := range []string{"early-close", "read-all", "init-error", "decoder-close-error"} {
						t.Run(mode, func(t *testing.T) {
							dir, err := ioutil.TempDir("", "diskv-read-close-")
							if err != nil {
								t.Fatal(err)
							}
							defer os.RemoveAll(dir)
							c := &captureCompression{Compression: tc.compression}
							d := New(Options{BasePath: dir, Compression: c, CacheSizeMax: cacheSize})
							value := []byte("stored value to read")
							if err := d.Write("key", value); err != nil {
								t.Fatal(err)
							}
							failure := errors.New("decoder failure")
							if mode == "init-error" {
								c.initErr = failure
							}
							if mode == "decoder-close-error" {
								c.closeErr = failure
							}
							r, err := d.ReadStream("key", false)
							if mode == "init-error" {
								if err != failure {
									t.Fatalf("initialization error = %v, want %v", err, failure)
								}
							} else {
								if err != nil {
									t.Fatal(err)
								}
								if mode == "read-all" {
									got, err := ioutil.ReadAll(r)
									if err != nil || !bytes.Equal(got, value) {
										t.Fatalf("ReadAll = %q, %v", got, err)
									}
								}
								err = r.Close()
								if mode == "decoder-close-error" {
									if err != failure || c.decoderCloses != 1 {
										t.Errorf("Close = %v, decoder closes = %d", err, c.decoderCloses)
									}
								} else if err != nil {
									t.Errorf("Close = %v", err)
								}
							}
							assertSourceClosed(t, c.source)
						})
					}
				})
			}
		})
	}
}

func TestClosingReaderClosesOnce(t *testing.T) {
	for _, drain := range []bool{false, true} {
		name := "early-close"
		if drain {
			name = "eof"
		}
		t.Run(name, func(t *testing.T) {
			closes := 0
			r := &closingReader{rc: &closeSpy{Reader: bytes.NewBufferString("value"), closes: &closes}}
			closer, ok := interface{}(r).(io.Closer)
			if !ok {
				t.Fatal("reader does not implement Close")
			}
			if drain {
				if _, err := ioutil.ReadAll(r); err != nil {
					t.Fatal(err)
				}
				if closes != 1 {
					t.Errorf("EOF did not close reader: %d closes", closes)
				}
			}
			for i := 0; i < 2; i++ {
				if err := closer.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if closes != 1 {
				t.Errorf("got %d closes, want 1", closes)
			}
		})
	}
}

func TestReadStreamCloseAllowsReplacement(t *testing.T) {
	for _, cacheSize := range []uint64{0, 1024} {
		dir, err := ioutil.TempDir("", "diskv-read-replace-")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		d := New(Options{BasePath: filepath.Join(dir, "store"), TempDir: filepath.Join(dir, "temp"), CacheSizeMax: cacheSize})
		if err := d.Write("key", []byte("old")); err != nil {
			t.Fatal(err)
		}
		r, err := d.ReadStream("key", false)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if d.isCached("key") {
			t.Error("early close cached an unread value")
		}
		if err := d.Write("key", []byte("new")); err != nil {
			t.Fatalf("replacement after Close: %v", err)
		}
	}
}

func TestClosingReaderCloseErrors(t *testing.T) {
	decoderErr := errors.New("decoder close")
	sourceErr := errors.New("source close")
	for _, tc := range []struct {
		name                        string
		decoderErr, sourceErr, want error
	}{
		{"success", nil, nil, nil},
		{"decoder-error", decoderErr, nil, decoderErr},
		{"source-error", nil, sourceErr, sourceErr},
		{"both-errors", decoderErr, sourceErr, decoderErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, drain := range []bool{false, true} {
				name := "close"
				if drain {
					name = "eof"
				}
				t.Run(name, func(t *testing.T) {
					decoderCloses, sourceCloses := 0, 0
					r := &closingReader{
						rc:     &closeSpy{Reader: bytes.NewBufferString("value"), closes: &decoderCloses, closeErr: tc.decoderErr},
						source: &closeSpy{closes: &sourceCloses, closeErr: tc.sourceErr},
					}
					var err error
					if drain {
						_, err = ioutil.ReadAll(r)
					} else {
						err = r.Close()
					}
					if err != tc.want {
						t.Errorf("error = %v, want %v", err, tc.want)
					}
					if err := r.Close(); err != nil {
						t.Errorf("second close = %v", err)
					}
					if decoderCloses != 1 || sourceCloses != 1 {
						t.Errorf("close counts: decoder=%d source=%d, want 1 each", decoderCloses, sourceCloses)
					}
				})
			}
		})
	}
}

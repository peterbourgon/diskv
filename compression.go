package diskv

import (
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
	"io/ioutil"
)

// Compression is an interface that Diskv uses to implement compression of
// data. You may define these methods on your own type, or use one of the
// NewCompression helpers.
type Compression interface {
	Writer(dst io.Writer) (io.WriteCloser, error)
	Reader(src io.Reader) (io.ReadCloser, error)
}

// TODO
type GenericCompression struct {
	wf func(w io.Writer) (io.WriteCloser, error)
	rf func(r io.Reader) (io.ReadCloser, error)
}

// TODO
func (g *GenericCompression) Writer(dst io.Writer) (io.WriteCloser, error) {
	return g.wf(dst)
}

// TODO
func (g *GenericCompression) Reader(src io.Reader) (io.ReadCloser, error) {
	return g.rf(src)
}

type nopCloser struct{ io.Writer }

func (w nopCloser) Close() error { return nil }

//
//
//

func NewNopCompression() Compression {
	return &GenericCompression{
		wf: func(w io.Writer) (io.WriteCloser, error) { return nopCloser{w}, nil },
		rf: func(r io.Reader) (io.ReadCloser, error) { return ioutil.NopCloser(r), nil },
	}
}

func NewGzipCompression() Compression {
	return NewGzipCompressionLevel(flate.DefaultCompression)
}

func NewGzipCompressionLevel(level int) Compression {
	return &GenericCompression{
		wf: func(w io.Writer) (io.WriteCloser, error) { return gzip.NewWriterLevel(w, level) },
		rf: func(r io.Reader) (io.ReadCloser, error) { return gzip.NewReader(r) },
	}
}

func NewZlibCompression() Compression {
	return NewZlibCompressionLevel(flate.DefaultCompression)
}

func NewZlibCompressionLevel(level int) Compression {
	return NewZlibCompressionLevelDict(level, nil)
}

func NewZlibCompressionLevelDict(level int, dict []byte) Compression {
	return &GenericCompression{
		func(w io.Writer) (io.WriteCloser, error) { return zlib.NewWriterLevelDict(w, level, dict) },
		func(r io.Reader) (io.ReadCloser, error) { return zlib.NewReaderDict(r, dict) },
	}
}

package diskv

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"io"
)

// Compression defines an interface that Diskv uses to implement compression of
// data. You may define these methods on your own type, or use one of the
// NewCompression helpers.
type Compression interface {
	Compress(dst io.Writer, src io.Reader) error
	Decompress(dst io.Writer, src io.Reader) error
}

// compress uses the passed Compression to compress the given value buffer
// and returns the compressed output.
func compress(c Compression, val []byte) ([]byte, error) {
	dst, src := &bytes.Buffer{}, bytes.NewBuffer(val)
	if err := c.Compress(dst, src); err != nil {
		return []byte{}, err
	}
	return dst.Bytes(), nil
}

// decompress uses the passed Compression to decompress the given value buffer
// and returns the decompressed output.
func decompress(c Compression, val []byte) ([]byte, error) {
	dst, src := &bytes.Buffer{}, bytes.NewBuffer(val)
	if err := c.Decompress(dst, src); err != nil {
		return []byte{}, err
	}
	return dst.Bytes(), nil
}

//
//
//

// ReaderFunc yields an io.ReadCloser which should perform
// decompression from the passed io.Reader.
type ReaderFunc func(r io.Reader) (io.ReadCloser, error)

// WriterFunc yields an io.WriteCloser which should perform
// compression into the passed io.Writer.
type WriterFunc func(w io.Writer) (io.WriteCloser, error)

// A GenericCompression implements Diskv's Compression interface. Users must
// supply it with two functions: a WriterFunc, which wraps an io.Writer with a
// compression layer, and a ReaderFunc, which wraps an io.Reader with a
// decompression layer.
type GenericCompression struct {
	wf WriterFunc
	rf ReaderFunc
}

// NewCompression returns a GenericCompression from the passed Writer
// and Reader functions, which you may supply directly.
//
// You may also use one of the NewCompression helpers, which
// automatically provide Writer and Reader functions for some of
// the stdlib compression algorithms.
func NewCompression(wf WriterFunc, rf ReaderFunc) *GenericCompression {
	return &GenericCompression{
		wf: wf,
		rf: rf,
	}
}

func (c *GenericCompression) Compress(dst io.Writer, src io.Reader) error {
	w, err := c.wf(dst)
	if err != nil {
		return err
	}
	if _, err = io.Copy(w, src); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return nil
}

func (c *GenericCompression) Decompress(dst io.Writer, src io.Reader) error {
	r, err := c.rf(src)
	if err != nil {
		return err
	}
	if _, err = io.Copy(dst, r); err != nil {
		return err
	}
	if err = r.Close(); err != nil {
		return err
	}
	return nil
}

//
//
//

func NewGzipCompression() Compression {
	return NewGzipCompressionLevel(flate.DefaultCompression)
}

func NewGzipCompressionLevel(level int) Compression {
	return NewCompression(
		func(w io.Writer) (io.WriteCloser, error) { return gzip.NewWriterLevel(w, level) },
		func(r io.Reader) (io.ReadCloser, error) { return gzip.NewReader(r) },
	)
}

func NewZlibCompression() Compression {
	return NewZlibCompressionLevel(flate.DefaultCompression)
}

func NewZlibCompressionLevel(level int) Compression {
	return NewZlibCompressionLevelDict(level, nil)
}

func NewZlibCompressionLevelDict(level int, dict []byte) Compression {
	return NewCompression(
		func(w io.Writer) (io.WriteCloser, error) { return zlib.NewWriterLevelDict(w, level, dict) },
		func(r io.Reader) (io.ReadCloser, error) { return zlib.NewReaderDict(r, dict) },
	)
}

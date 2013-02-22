// Diskv (disk-vee) is a simple, persistent key-value store.
// It stores all data flatly on the filesystem.

package diskv

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	defaultBasePath             = "diskv"
	defaultFilePerm os.FileMode = 0666
	defaultPathPerm os.FileMode = 0777
)

var (
	defaultTransform = func(s string) []string { return []string{""} }
)

// A TransformFunc transforms a key into a slice of strings, with each
// element in the slice representing a directory in the file path
// where the key's entry will eventually be stored.
//
// For example, if TransformFunc transforms "abcdef" to ["ab", "cde", "f"],
// the final location of the data file will be <basedir>/ab/cde/f/abcdef
type TransformFunction func(s string) []string

// Options define a set of properties that dictate Diskv behavior.
// All values are optional.
type Options struct {
	BasePath     string
	Transform    TransformFunction
	CacheSizeMax uint64 // bytes
	PathPerm     os.FileMode
	FilePerm     os.FileMode

	Index     Index
	IndexLess LessFunction

	Compression Compression
}

// Diskv implements the Diskv interface. You shouldn't construct Diskv
// structures directly; instead, use the New constructor.
type Diskv struct {
	sync.RWMutex
	Options
	cache     map[string][]byte
	cacheSize uint64
}

// New returns an initialized Diskv structure, ready to use.
// If the path identified by baseDir already contains data,
// it will be accessible, but not yet cached.
func New(options Options) *Diskv {
	if options.BasePath == "" {
		options.BasePath = defaultBasePath
	}
	if options.Transform == nil {
		options.Transform = defaultTransform
	}
	if options.PathPerm == 0 {
		options.PathPerm = defaultPathPerm
	}
	if options.FilePerm == 0 {
		options.FilePerm = defaultFilePerm
	}

	d := &Diskv{
		Options:   options,
		cache:     map[string][]byte{},
		cacheSize: 0,
	}

	if d.Index != nil && d.IndexLess != nil {
		d.Index.Initialize(d.IndexLess, d.Keys())
	}

	return d
}

// Write synchronously writes the key-value pair to disk, making it immediately
// available for reads. Write relies on the filesystem to perform an eventual
// sync to physical media. If you need stronger guarantees, use WriteAndSync.
func (d *Diskv) Write(key string, val []byte) error {
	return d.write(key, bytes.NewBuffer(val), false)
}

func (d *Diskv) WriteStream(key string, reader io.Reader) error {
	return d.write(key, reader, false)
}

// WriteAndSync does the same thing as Write, but explicitly calls
// Sync on the relevant file descriptor.
func (d *Diskv) WriteAndSync(key string, val []byte) error {
	return d.write(key, bytes.NewBuffer(val), true)
}

// write synchronously writes the key-value pair to disk,
// making it immediately available for reads. write optionally
// performs a Sync on the relevant file descriptor.
func (d *Diskv) write(key string, reader io.Reader, sync bool) error {
	if len(key) <= 0 {
		return fmt.Errorf("empty key")
	}

	d.Lock()
	defer d.Unlock()
	if err := d.ensurePath(key); err != nil {
		return err
	}

	mode := os.O_WRONLY | os.O_CREATE | os.O_TRUNC // overwrite if exists
	f, err := os.OpenFile(d.completeFilename(key), mode, d.FilePerm)
	if err != nil {
		return err
	}

	if err = d.maybeWriteCompressed(f, reader); err != nil {
		f.Close() // error deliberately ignored
		return err
	}

	if sync {
		if err := f.Sync(); err != nil {
			f.Close() // error deliberately ignored
			return err
		}
	}

	if err := f.Close(); err != nil {
		return err
	}

	if d.Index != nil {
		d.Index.Insert(key)
	}

	delete(d.cache, key) // cache only on read
	return nil
}

// Read reads the key and returns the value.
// If the key is available in the cache, Read won't touch the disk.
// If the key is not in the cache, Read will have the side-effect of
// lazily caching the value.
func (d *Diskv) Read(key string) ([]byte, error) {
	d.RLock()
	defer d.RUnlock()

	// check cache first
	if val, ok := d.cache[key]; ok {
		return d.decompress(val)
	}

	// read from disk
	val, err := ioutil.ReadFile(d.completeFilename(key))
	if err != nil {
		return []byte{}, err
	}

	// cache lazily
	go d.cacheWithoutLock(key, val)

	// return
	return d.decompress(val)
}

func (d *Diskv) ReadStream(key string, writer io.Writer) error {
	d.RLock()
	defer d.RUnlock()

	// read from disk
	f, err := os.Open(d.completeFilename(key))
	if err != nil {
		return err
	}
	if err = d.maybeReadDecompressed(writer, f); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Erase synchronously erases the given key from the disk and the cache.
func (d *Diskv) Erase(key string) error {
	d.Lock()
	defer d.Unlock()

	// erase from cache
	if val, ok := d.cache[key]; ok {
		d.cacheSize -= uint64(len(val))
		delete(d.cache, key)
	}

	// erase from index
	if d.Index != nil {
		d.Index.Delete(key)
	}

	// erase from disk
	filename := d.completeFilename(key)
	if s, err := os.Stat(filename); err == nil {
		if !!s.IsDir() {
			return fmt.Errorf("bad key")
		}
		if err = os.Remove(filename); err != nil {
			return err
		}
	} else {
		return err
	}

	// clean up and return
	d.pruneDirs(key)
	return nil
}

// EraseAll will delete all of the data from the store, both
// in the cache and on the disk. Note that EraseAll doesn't
// distinguish diskv-related data from non-diskv-related data.
// Care should be taken to always specify a diskv base directory
// that is exclusively for diskv data.
func (d *Diskv) EraseAll() error {
	d.Lock()
	defer d.Unlock()
	d.cache = make(map[string][]byte)
	d.cacheSize = 0
	return os.RemoveAll(d.BasePath)
}

// Keys returns a channel that will yield every key
// accessible by the store in undefined order.
func (d *Diskv) Keys() <-chan string {
	c := make(chan string)
	go func() {
		filepath.Walk(d.BasePath, walker(c))
		close(c)
	}()
	return c
}

//
//
//

func (d *Diskv) compress(val []byte) ([]byte, error) {
	if d.Compression != nil {
		return compress(d.Compression, val)
	}
	return val, nil
}

func (d *Diskv) decompress(val []byte) ([]byte, error) {
	if d.Compression != nil {
		return decompress(d.Compression, val)
	}
	return val, nil
}

func (d *Diskv) maybeWriteCompressed(writer io.Writer, reader io.Reader) error {
	if d.Compression != nil {
		return d.Compression.Compress(writer, reader)
	}
	_, err := io.Copy(writer, reader)
	return err
}

func (d *Diskv) maybeReadDecompressed(writer io.Writer, reader io.Reader) error {
	if d.Compression != nil {
		return d.Compression.Decompress(writer, reader)
	}
	_, err := io.Copy(writer, reader)
	return err
}

// walker returns a function which satisfies the filepath.WalkFunc interface.
// It sends every non-directory file entry down the channel c.
func walker(c chan string) func(path string, info os.FileInfo, err error) error {
	return func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			c <- info.Name()
		}
		return nil // "pass"
	}
}

// pathFor returns the absolute path for location on the filesystem
// where the data for the given key will be stored.
func (d *Diskv) pathFor(key string) string {
	return fmt.Sprintf(
		"%s%c%s",
		d.BasePath,
		os.PathSeparator,
		strings.Join(d.Transform(key), string(os.PathSeparator)),
	)
}

// ensureDir is a helper function that generates all necessary
// directories on the filesystem for the given key.
func (d *Diskv) ensurePath(key string) error {
	return os.MkdirAll(d.pathFor(key), d.PathPerm)
}

// completeFilename returns the absolute path to the file for the given key.
func (d *Diskv) completeFilename(key string) string {
	return fmt.Sprintf("%s%c%s", d.pathFor(key), os.PathSeparator, key)
}

// cacheWithLock attempts to cache the given key-value pair in the
// store's cache. It can fail if the value is larger than the cache's
// maximum size.
func (d *Diskv) cacheWithLock(key string, val []byte) error {
	valueSize := uint64(len(val))
	if err := d.ensureCacheSpaceFor(valueSize); err != nil {
		return fmt.Errorf("%s; not caching", err)
	}

	if (d.cacheSize + valueSize) > d.CacheSizeMax {
		panic(
			fmt.Sprintf(
				"failed to make room for value (%d/%d)",
				valueSize,
				d.CacheSizeMax,
			),
		)
	}

	d.cache[key] = val
	d.cacheSize += valueSize
	return nil
}

// cacheWithoutLock acquires the store's (write) mutex
// and calls cacheWithLock.
func (d *Diskv) cacheWithoutLock(key string, val []byte) error {
	d.Lock()
	defer d.Unlock()
	return d.cacheWithLock(key, val)
}

// pruneDirs deletes empty directories in the path walk leading to the key k.
// Typically this function is called after an Erase is made.
func (d *Diskv) pruneDirs(key string) error {
	pathlist := d.Transform(key)
	for i := range pathlist {
		pslice := pathlist[:len(pathlist)-i]
		dir := fmt.Sprintf(
			"%s%c%s",
			d.BasePath,
			os.PathSeparator,
			strings.Join(pslice, string(os.PathSeparator)),
		)

		// thanks to Steven Blenkinsop for this snippet
		switch fi, err := os.Stat(dir); true {
		case err != nil:
			return err
		case !fi.IsDir():
			panic(fmt.Sprintf("corrupt dirstate at %s", dir))
		}

		nlinks, err := filepath.Glob(fmt.Sprintf("%s%c*", dir, os.PathSeparator))
		if err != nil {
			return err
		} else if len(nlinks) > 0 {
			return nil // has subdirs -- do not prune
		}
		if err = os.Remove(dir); err != nil {
			return err
		}
	}

	return nil
}

// ensureCacheSpaceFor deletes entries from the cache in arbitrary order
// until the cache has at least valueSize bytes available.
func (d *Diskv) ensureCacheSpaceFor(valueSize uint64) error {
	if valueSize > d.CacheSizeMax {
		return fmt.Errorf(
			"value size (%d bytes) too large for cache (%d bytes)",
			valueSize,
			d.CacheSizeMax,
		)
	}

	safe := func() bool { return (d.cacheSize + valueSize) <= d.CacheSizeMax }
	for key, val := range d.cache {
		if safe() {
			break
		}
		delete(d.cache, key)            // delete is safe, per spec
		d.cacheSize -= uint64(len(val)) // len should return uint :|
	}
	if !safe() {
		panic(fmt.Sprintf(
			"%d bytes still won't fit in the cache! (max %d bytes)",
			valueSize,
			d.CacheSizeMax,
		))
	}

	return nil
}

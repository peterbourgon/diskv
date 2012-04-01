package diskv

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	defaultFilePerm os.FileMode = 0777
	defaultDirPerm  os.FileMode = 0777
)

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

// A TransformFunc transforms a key into a slice of strings, with each
// element in the slice representing a directory in the file path
// where the key's entry will eventually be stored.
type TransformFunc func(string) []string

type Store struct {
	baseDir      string
	xf           TransformFunc
	cache        map[string][]byte
	cacheSize    uint // bytes
	cacheSizeMax uint
	mutex        sync.RWMutex
}

// NewStore returns a new, unordered diskv store.
// If the path identified by baseDir already contains data,
// it will be accessible (but not yet cached) by this store.
func NewStore(baseDir string, xf TransformFunc, cacheSizeMax uint) *Store {
	s := &Store{
		baseDir:      baseDir,
		xf:           xf,
		cache:        map[string][]byte{},
		cacheSize:    0,
		cacheSizeMax: cacheSizeMax,
		mutex:        sync.RWMutex{},
	}
	return s
}

// Keys returns a channel that will yield every key
// accessible by the store in undefined order.
func (s *Store) Keys() <-chan string {
	c := make(chan string)
	go func() {
		filepath.Walk(s.baseDir, walker(c))
		close(c)
	}()
	return c
}

// Flush will delete all of the data from the store, both
// in the cache and on the disk. Note that Flush doesn't
// distinguish diskv-related data from non-diskv-related data.
// Care should be taken to always specify a diskv base directory
// that is exclusively for diskv data.
func (s *Store) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.cache = make(map[string][]byte)
	s.cacheSize = 0
	return os.RemoveAll(s.baseDir)
}

// Write synchronously writes the key-value pair to disk,
// making it immediately available for reads.
func (s *Store) Write(k string, v []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if len(k) <= 0 {
		return fmt.Errorf("empty key")
	}
	if err := s.ensureDir(k); err != nil {
		return err
	}
	mode := os.O_WRONLY | os.O_CREATE // overwrite if exists
	f, err := os.OpenFile(s.filename(k), mode, defaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(v); err != nil {
		return err
	}
	return nil // cache only on read
}

// Read reads the key and returns the value.
// If the key is available in the cache, Read won't touch the disk.
// If the key is not in the cache, Read will have the side-effect of
// lazily caching the value.
func (s *Store) Read(k string) ([]byte, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	// check cache first
	if v, ok := s.cache[k]; ok {
		return v, nil
	}
	// read from disk
	v, err := ioutil.ReadFile(s.filename(k))
	if err != nil {
		return []byte{}, err
	}
	// cache lazily
	go s.cacheWithoutLock(k, v)
	return v, nil
}

// Erase synchronously erases the given key from the disk and the cache.
func (s *Store) Erase(k string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// erase from cache
	if v, ok := s.cache[k]; ok {
		s.cacheSize -= uint(len(v))
		delete(s.cache, k)
	}
	// erase from disk
	filename := s.filename(k)
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
	// clean up
	s.pruneDirs(k)
	return nil
}

// IsCached returns true if the key exists in the cache.
func (s *Store) IsCached(k string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, present := s.cache[k]
	return present
}

// ensureDir is a helper function that generates all necessary
// directories on the filesystem for the given key.
func (s *Store) ensureDir(k string) error {
	return os.MkdirAll(s.dir(k), defaultDirPerm)
}

// dir returns the absolute path for location on the filesystem
// where the data for the given key will be stored.
func (s *Store) dir(k string) string {
	pathlist := s.xf(k)
	return fmt.Sprintf("%s/%s", s.baseDir, strings.Join(pathlist, "/"))
}

// filename returns the absolute path to the file for the given key.
func (s *Store) filename(k string) string {
	return fmt.Sprintf("%s/%s", s.dir(k), k)
}

// cacheWithLock attempts to cache the given key-value pair in the
// store's cache. It can fail if the value is larger than the cache's
// maximum size.
func (s *Store) cacheWithLock(k string, v []byte) error {
	valueSize := uint(len(v))
	if err := s.ensureCacheSpaceFor(valueSize); err != nil {
		return fmt.Errorf("%s; not caching", err)
	}
	if (s.cacheSize + valueSize) > s.cacheSizeMax {
		panic(
			fmt.Sprintf(
				"failed to make room for value (%d/%d)",
				valueSize,
				s.cacheSizeMax,
			),
		)
	}
	s.cache[k] = v
	s.cacheSize += valueSize
	return nil
}

// cacheWithoutLock acquires the store's (write) mutex
// and calls cacheWithLock.
func (s *Store) cacheWithoutLock(k string, v []byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.cacheWithLock(k, v)
}

// ensureCacheSpaceFor deletes entries from the cache in arbitrary order
// until the cache has at least valueSize bytes available.
func (s *Store) ensureCacheSpaceFor(valueSize uint) error {
	if valueSize > s.cacheSizeMax {
		return fmt.Errorf(
			"value size (%d bytes) too large for cache (%d bytes)",
			valueSize,
			s.cacheSizeMax,
		)
	}
	safe := func() bool { return (s.cacheSize + valueSize) <= s.cacheSizeMax }
	for k, v := range s.cache {
		if safe() {
			break
		}
		delete(s.cache, k)          // delete is safe, per spec
		s.cacheSize -= uint(len(v)) // len should return uint :|
	}
	if !safe() {
		panic(fmt.Sprintf(
			"%d bytes still won't fit in the cache! (max %d bytes)",
			valueSize,
			s.cacheSizeMax,
		))
	}
	return nil
}

// pruneDirs deletes empty directories in the path walk leading to the key k.
// Typically this function is called after an Erase is made.
func (s *Store) pruneDirs(k string) error {
	pathlist := s.xf(k)
	for i := range pathlist {
		pslice := pathlist[:len(pathlist)-i]
		dir := fmt.Sprintf("%s/%s", s.baseDir, strings.Join(pslice, "/"))
		// thanks to Steven Blenkinsop for this snippet
		switch fi, err := os.Stat(dir); true {
		case err != nil:
			return err
		case !fi.IsDir():
			panic(fmt.Sprintf("corrupt dirstate at %s", dir))
		}
		nlinks, err := filepath.Glob(fmt.Sprintf("%s/*", dir))
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

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

func walker(c chan string) func(path string, info os.FileInfo, err error) error {
	return func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			c <- info.Name()
		}
		return nil // "pass"
	}
}

type TransformFunc func(string) []string

type Store struct {
	baseDir      string
	xf           TransformFunc
	cache        map[string][]byte
	cacheSize    uint // bytes
	cacheSizeMax uint
	mutex        sync.RWMutex
}

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

func (s *Store) Keys() <-chan string {
	c := make(chan string)
	go func() {
		filepath.Walk(s.baseDir, walker(c))
		close(c)
	}()
	return c
}

func (s *Store) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.cache = make(map[string][]byte)
	s.cacheSize = 0
	return os.RemoveAll(s.baseDir)
}

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

func (s *Store) IsCached(k string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	_, present := s.cache[k]
	return present
}

func (s *Store) ensureDir(k string) error {
	return os.MkdirAll(s.dir(k), defaultDirPerm)
}

func (s *Store) dir(k string) string {
	pathlist := s.xf(k)
	return fmt.Sprintf("%s/%s", s.baseDir, strings.Join(pathlist, "/"))
}

// Returns the full path to the file holding data for the given Key.
func (s *Store) filename(k string) string {
	return fmt.Sprintf("%s/%s", s.dir(k), k)
}

func (s *Store) cacheWithLock(k string, v []byte) {
	valueSize := uint(len(v))
	if valueSize > s.cacheSizeMax {
		return // cannot comply
	}
	s.ensureCacheSpaceFor(valueSize)
	if (s.cacheSize + valueSize) <= s.cacheSizeMax {
		s.cache[k] = v
		s.cacheSize += valueSize
	}
}

func (s *Store) cacheWithoutLock(k string, v []byte) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.cacheWithLock(k, v)
}

// Deletes entries from the cache until it has at least sz bytes available.
func (s *Store) ensureCacheSpaceFor(valueSize uint) {
	for k, v := range s.cache {
		if (s.cacheSize + valueSize) <= s.cacheSizeMax {
			break
		}
		delete(s.cache, k)          // delete is safe, per spec
		s.cacheSize -= uint(len(v)) // len should return uint :|
	}
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

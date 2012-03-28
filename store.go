package diskv

/*

import (
	"errors"
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type DStore struct {
	basedir    string
	xf         TransformFunc
	fileperm   os.FileMode
	dirperm    os.FileMode
	cache      map[Key]Value
	mutex      *sync.RWMutex
	cachesz    uint
	cachemaxsz uint
	index      *llrb.Tree
	lf         llrb.LessFunc
}

// Returns a new, unordered Store that represents the data in the given
// basedir directory. The cache will grow to a maximum maxsz bytes.
func NewDStore(basedir string, xf TransformFunc, maxsz uint) *DStore {
	s := &DStore{}
	s.basedir = fmt.Sprintf("%s/%s", basedir, "diskv")
	s.xf = xf
	s.fileperm, s.dirperm = 0777, 0777
	s.cache = make(map[Key]Value)
	s.mutex = &sync.RWMutex{}
	s.cachesz, s.cachemaxsz = 0, maxsz
	s.index, s.lf = nil, nil
	return s
}

// Returns a new, ordered Store that represents the data in the given
// basedir directory. The directory tree at basedir will be scanned at
// initialization time, to populate the index with existing Keys.
func NewODStore(basedir string, xf TransformFunc, maxsz uint, lf llrb.LessFunc) *DStore {
	s := NewDStore(basedir, xf, maxsz)
	s.index = llrb.New(lf)
	s.lf = lf
	s.populateIndex()
	return s
}

func (s *DStore) populateIndex() {
	t1 := time.Now()
	c := s.Keys()
	for k := <-c; len(k) > 0; k = <-c {
		s.index.ReplaceOrInsert(k)
	}
	td := time.Now().Sub(t1)
	log.Printf("index populated with %d elements in %d ms\n", s.index.Len(), td)
}

// Returns the directory that will hold the given Key.
func (s *DStore) dir(k Key) string {
	pathlist := s.xf(k)
	return fmt.Sprintf("%s/%s", s.basedir, strings.Join(pathlist, "/"))
}

// Returns the full path to the file holding data for the given Key.
func (s *DStore) filename(k Key) string {
	return fmt.Sprintf("%s/%s", s.dir(k), k)
}

// Creates all necessary directories on disk to hold the given Key.
func (s *DStore) ensureDir(k Key) error {
	return os.MkdirAll(s.dir(k), s.dirperm)
}

// Deletes empty directories in the path walk leading to the Key k.
// Typically this function is called after an Erase() is made.
func (s *DStore) pruneDirs(k Key) error {
	pathlist := s.xf(k)
	for i := range pathlist {
		pslice := pathlist[:len(pathlist)-i]
		dir := fmt.Sprintf("%s/%s", s.basedir, strings.Join(pslice, "/"))
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

// Deletes entries from the cache until it has at least sz bytes available.
func (s *DStore) ensureSpace(sz uint) {
	for k, v := range s.cache {
		if (s.cachesz + sz) <= s.cachemaxsz {
			break
		}
		s.cachesz -= uint(len(v)) // len should return uint :|
		delete(s.cache, k)        // delete is safe, per spec
	}
}

func (s *DStore) cacheWithLock(k Key, v Value) {
	vsz := uint(len(v))
	s.ensureSpace(vsz)
	if (s.cachesz + vsz) <= s.cachemaxsz {
		s.cache[k] = v
		s.cachesz += vsz
	}
	if s.index != nil {
		s.index.ReplaceOrInsert(k)
	}
}

func (s *DStore) cacheWithoutLock(k Key, v Value) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.cacheWithLock(k, v)
}

// Writes the Value to the store under the given Key.
// Overwrites anything already existing under the given Key.
func (s *DStore) Write(k Key, v Value) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if len(k) <= 0 {
		return errors.New("empty key")
	}
	// write the data to disk (overwrite if exists)
	if err := s.ensureDir(k); err != nil {
		return err
	}
	mode := os.O_WRONLY | os.O_CREATE
	if f, err := os.OpenFile(s.filename(k), mode, s.fileperm); err == nil {
		defer f.Close()
		if _, err = f.Write(v); err != nil {
			return err
		}
	} else {
		return err
	}
	// index immediately
	if s.index != nil {
		s.index.ReplaceOrInsert(k)
	}
	// cache only on read
	return nil
}

// Returns the Value stored under the given Key, if it exists.
// Returns error if the Key doesn't exist.
func (s *DStore) Read(k Key) (Value, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	// check cache first
	if v, ok := s.cache[k]; ok {
		return v, nil
	}
	// read from disk
	v, err := ioutil.ReadFile(s.filename(k))
	if err != nil {
		return Value{}, err
	}
	// cache lazily
	go s.cacheWithoutLock(k, v)
	return v, nil
}

// Erases the Value stored under the given Key, if it exists.
// Returns error if the Key doesn't exist.
func (s *DStore) Erase(k Key) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// erase from cache
	if v, ok := s.cache[k]; ok {
		s.cachesz -= uint(len(v))
		delete(s.cache, k)
	}
	// erase from index
	if s.index != nil {
		s.index.Delete(k)
	}
	// erase from disk
	filename := s.filename(k)
	if s, err := os.Stat(filename); err == nil {
		if !!s.IsDir() {
			return errors.New("bad key")
		}
		if err = os.Remove(filename); err != nil {
			return err
		}
	} else {
		return err
	}
	if s.index != nil {
		s.index.Delete(k)
	}
	// clean up
	s.pruneDirs(k)
	return nil
}

// Erase all Keys and Values from the DStore.
func (s *DStore) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// flush cache
	s.cache = make(map[Key]Value)
	s.cachesz = 0
	// flush index
	if s.index != nil {
		s.index.Init(s.lf)
	}
	// flush disk
	return os.RemoveAll(s.basedir)
}

func visitor(c chan Key) func(path string, info os.FileInfo, err error) error {
	return func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			c <- Key(info.Name())
		}
		return nil // "pass"
	}
}

// Returns channel generating list of all keys in the store,
// based on the state of the disk. No order guarantee.
func (s *DStore) Keys() <-chan Key {
	c := make(chan Key)
	go func() {
		filepath.Walk(s.basedir, visitor(c))
		close(c)
	}()
	return c
}

func (s *DStore) IsCached(k Key) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, present := s.cache[k]
	return present
}

func (s *DStore) IsIndexed(k Key) bool {
	if s.index != nil {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		return s.index.Has(k)
	}
	return false
}

// Return a maximum of count keys from the index, starting at 
// one beyond the given key and iterating forward
func (s *DStore) KeysFrom(k Key, count int) ([]Key, error) {
	if s.index == nil {
		panic("KeysFrom cannot be called on non-ordered store")
	}
	if s.index.Len() <= 0 {
		return []Key{}, nil
	}
	skip_first := true
	if len(k) <= 0 || !s.index.Has(k) {
		k = s.index.Min().(Key) // no such key, so start at the top
		skip_first = false
	}
	keys := make([]Key, count)
	c := s.index.IterRange(k, s.index.Max())
	total := 0
	if skip_first {
		<-c
	}
	for i, k := 0, <-c; i < count && k != nil; i, k = i+1, <-c {
		keys[i] = k.(Key)
		total++
	}
	if total < count { // hack to get around IterRange returning only E < @upper
		keys[total] = s.index.Max().(Key)
		total++
	}
	keys = keys[:total]
	return keys, nil
}

*/

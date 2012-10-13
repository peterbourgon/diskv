package diskv

/*
import (
	"github.com/petar/GoLLRB/llrb"
	"log"
	"sync"
	"time"
)
*/

/*
type orderedStore struct {
	Store

	indexMutex sync.RWMutex
	index      *llrb.Tree
	lf         llrb.LessFunc
}

// NewOrderedStore returns a new, ordered diskv store.
// Existing keys are scanned and ordered at instantiation.
func NewOrderedStore(baseDir string, xf TransformFunc, cacheSizeMax uint) Store {
	s := &orderedStore{
		Store:      NewBasicStore(baseDir, xf, cacheSizeMax),
		indexMutex: sync.RWMutex{},
		index:      nil,
		lf:         nil,
	}
	defaultLess := func(a, b interface{}) bool { return a.(string) < b.(string) }
	s.ResetOrder(defaultLess)
	return s
}

// KeysFrom yields a maximum of count keys on the returned channel, in order.
// If the passed key is empty, KeysFrom will return the first count keys.
// If the passed key is non-empty, the first key in the returned slice will
// be the key that immediately follows the passed key, in key order.
//
// KeysFrom is designed to effect a simple "pagination" of keys.
func (s *orderedStore) KeysFrom(key string, count int) <-chan string {
	if s.index.Len() <= 0 {
		return nil // receiving on a nil chan is like it's immediately closed
	}

	skipFirst := true
	if len(key) <= 0 || !s.index.Has(key) {
		key = s.index.Min().(string) // no such key, so start at the top
		skipFirst = false
	}

	c0 := s.index.IterRange(key, s.index.Max())
	if skipFirst {
		<-c0
	}

	c := make(chan string)
	go func() {
		wasClosed, sent := false, 0
		for ; sent < count; sent++ {
			key, ok := <-c0
			if !ok {
				wasClosed = true
				break
			}
			c <- key.(string)
		}
		if wasClosed && sent < count {
			// hack to get around IterRange returning only E < @upper
			c <- s.index.Max().(string)
		}
		close(c)
	}()
	return c
}

// ResetOrder resets the key comparison function for the order index,
// and rebuilds the index according to that function.
func (s *orderedStore) ResetOrder(lf llrb.LessFunc) {
	s.indexMutex.Lock()
	defer s.indexMutex.Unlock()
	s.lf = lf
	s.rebuildIndex()
}

// rebuildIndex does the work of regenerating the index
// according to the comparison function in the store.
func (s *orderedStore) rebuildIndex() {
	s.index = llrb.New(s.lf)
	keyChan := s.Keys()
	count, began := 0, time.Now()
	for {
		key, ok := <-keyChan
		if !ok {
			break // closed
		}
		s.index.ReplaceOrInsert(key)
		count = count + 1
	}
	if count > 0 {
		log.Printf("index rebuilt (%d keys) in %s", count, time.Since(began))
	}
}

// Flush triggers a store Flush, and does extra work to clear the order index.
func (s *orderedStore) EraseAll() error {
	err := s.Store.EraseAll()
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.Init(s.lf)
	}
	return err
}

// Write triggers a store Write, and does extra work to update the order index.
func (s *orderedStore) Write(k string, v []byte) error {
	err := s.Store.Write(k, v)
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.ReplaceOrInsert(k)
	}
	return err
}

// Erase triggers a store Erase, and does extra work to update the order index.
func (s *orderedStore) Erase(k string) error {
	err := s.Store.Erase(k)
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.Delete(k)
	}
	return err
}

// IsIndex returns true if the given key exists in the order index.
func (s *orderedStore) IsIndexed(k string) bool {
	s.indexMutex.RLock()
	defer s.indexMutex.RUnlock()
	return s.index.Has(k)
}
*/

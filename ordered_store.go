package diskv

import (
	"github.com/petar/GoLLRB/llrb"
	"log"
	"sync"
	"time"
)

type OrderedStore struct {
	Store
	indexMutex sync.RWMutex
	index      *llrb.Tree
	lf         llrb.LessFunc
}

// NewOrderedStore returns a new, ordered diskv store.
// It abides the same semantics as NewStore.
// Existing keys are scanned and ordered at instantiation.
func NewOrderedStore(baseDir string, xf TransformFunc, cacheSizeMax uint) *OrderedStore {
	s := &OrderedStore{
		Store:      *NewStore(baseDir, xf, cacheSizeMax),
		indexMutex: sync.RWMutex{},
		index:      nil,
		lf:         nil,
	}
	defaultLess := func(a, b interface{}) bool { return a.(string) < b.(string) }
	s.ResetOrder(defaultLess)
	return s
}

// KeysFrom returns a slice of ordered keys that is maximum count items long.
// If the passed key is empty, KeysFrom will return the first count keys.
// If the passed key is non-empty, the first key in the returned slice will
// be the key that immediately follows the passed key, in key order.
// KeysFrom is designed to be used to effect a simple "pagination" of keys.
func (s *OrderedStore) KeysFrom(k string, count int) ([]string, error) {
	if s.index.Len() <= 0 {
		return []string{}, nil
	}
	skipFirst := true
	if len(k) <= 0 || !s.index.Has(k) {
		k = s.index.Min().(string) // no such key, so start at the top
		skipFirst = false
	}
	keys := make([]string, count)
	c := s.index.IterRange(k, s.index.Max())
	total := 0
	if skipFirst {
		<-c
	}
	for i, k := 0, <-c; i < count && k != nil; i, k = i+1, <-c {
		keys[i] = k.(string)
		total++
	}
	if total < count { // hack to get around IterRange returning only E < @upper
		keys[total] = s.index.Max().(string)
		total++
	}
	keys = keys[:total]
	return keys, nil
}

// ResetOrder resets the key comparison function for the order index, 
// and rebuilds the index according to that function.
func (s *OrderedStore) ResetOrder(lf llrb.LessFunc) {
	s.indexMutex.Lock()
	defer s.indexMutex.Unlock()
	s.lf = lf
	s.rebuildIndex()
}

// rebuildIndex does the work of regenerating the index
// according to the comparison function in the store.
func (s *OrderedStore) rebuildIndex() {
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
func (s *OrderedStore) Flush() error {
	err := s.Store.Flush()
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.Init(s.lf)
	}
	return err
}

// Write triggers a store Write, and does extra work to update the order index.
func (s *OrderedStore) Write(k string, v []byte) error {
	err := s.Store.Write(k, v)
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.ReplaceOrInsert(k)
	}
	return err
}

// Erase triggers a store Erase, and does extra work to update the order index.
func (s *OrderedStore) Erase(k string) error {
	err := s.Store.Erase(k)
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.Delete(k)
	}
	return err
}

// IsIndex returns true if the given key exists in the order index.
func (s *OrderedStore) IsIndexed(k string) bool {
	s.indexMutex.RLock()
	defer s.indexMutex.RUnlock()
	return s.index.Has(k)
}

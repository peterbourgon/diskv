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

func (s *OrderedStore) ResetOrder(lf llrb.LessFunc) {
	s.indexMutex.Lock()
	defer s.indexMutex.Unlock()
	s.lf = lf
	s.rebuildIndex()
}

func (s *OrderedStore) rebuildIndex() {
	s.index = llrb.New(s.lf)
	keyChan := s.Keys()
	count, begin := 0, time.Now()
	for {
		key, ok := <-keyChan
		if !ok {
			break // closed
		}
		s.index.ReplaceOrInsert(key)
		count = count + 1
	}
	if count > 0 {
		log.Printf("index rebuilt (%d keys) in %s", count, time.Now().Sub(begin))
	}
}

func (s *OrderedStore) Flush() error {
	err := s.Store.Flush()
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.Init(s.lf)
	}
	return err
}

func (s *OrderedStore) Write(k string, v []byte) error {
	err := s.Store.Write(k, v)
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.ReplaceOrInsert(k)
	}
	return err
}

func (s *OrderedStore) Erase(k string) error {
	err := s.Store.Erase(k)
	if err == nil {
		s.indexMutex.Lock()
		defer s.indexMutex.Unlock()
		s.index.Delete(k)
	}
	return err
}

func (s *OrderedStore) IsIndexed(k string) bool {
	s.indexMutex.RLock()
	defer s.indexMutex.RUnlock()
	return s.index.Has(k)
}

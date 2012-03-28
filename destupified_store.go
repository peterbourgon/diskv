package diskv

import (
	"github.com/petar/GoLLRB/llrb"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type TransformFunc func(string) []string

type Store struct {
	baseDir      string
	xf           TransformFunc
	cache        map[string][]byte
	cacheSize    uint // bytes
	cacheSizeMax uint
	mutex        sync.RWMutex
	index        *llrb.Tree
	lf           llrb.LessFunc
}

func NewStore(baseDir string, xf TransformFunc, cacheSizeMax uint) *Store {
	lessFunc := func(a, b interface{}) bool { return a.(string) < b.(string) }
	s := &Store{
		baseDir:      baseDir,
		xf:           xf,
		cache:        map[string][]byte{},
		cacheSize:    0,
		cacheSizeMax: cacheSizeMax,
		mutex:        sync.RWMutex{},
		index:        llrb.New(lessFunc),
		lf:           lessFunc,
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

func walker(c chan string) func(path string, info os.FileInfo, err error) error {
	return func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			c <- info.Name()
		}
		return nil // "pass"
	}
}

func (s *Store) ResetOrder(lf llrb.LessFunc) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lf = lf
	s.rebuildIndex()
}

func (s *Store) rebuildIndex() {
	keyChan := s.Keys()
	begin := time.Now()
	for {
		key, ok := <-keyChan
		if !ok {
			break // closed
		}
		s.index.ReplaceOrInsert(key)
	}
	log.Printf("index rebuilt in %s", time.Now().Sub(begin))
}

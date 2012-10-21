package diskv

import (
	"github.com/petar/GoLLRB/llrb"
	"sync"
)

// Index is a generic interface for things that can
// provide an ordered list of keys.
type Index interface {
	Initialize(less LessFunction, keys <-chan string)
	Insert(key string)
	Delete(key string)
	Keys(from string, n int) <-chan string
}

// LessFunction is used to initialize an Index of keys in a specific order.
type LessFunction func(string, string) bool

//
//
//

// LLRBIndex is an implementation of the Index interface
// using Petar Maymounkov's LLRB tree.
type LLRBIndex struct {
	sync.RWMutex
	tree *llrb.Tree
	less llrb.LessFunc
}

// Initialize populates the LLRB tree with data from the keys channel,
// according to the passed less function. It's destructive to the LLRBIndex.
func (i *LLRBIndex) Initialize(less LessFunction, keys <-chan string) {
	i.Lock()
	defer i.Unlock()

	llrbLess := convert(less)
	i.less = llrbLess
	i.tree = rebuild(llrbLess, keys)
}

// Insert inserts the given key (only) into the LLRB tree.
func (i *LLRBIndex) Insert(key string) {
	i.Lock()
	defer i.Unlock()
	if i.tree == nil || i.less == nil {
		panic("uninitialized index")
	}
	i.tree.ReplaceOrInsert(key)
}

// Delete removes the given key (only) from the LLRB tree.
func (i *LLRBIndex) Delete(key string) {
	i.Lock()
	defer i.Unlock()
	if i.tree == nil || i.less == nil {
		panic("uninitialized index")
	}
	i.tree.Delete(key)
}

// Keys yields a maximum of n keys on the returned channel, in order. It's
// designed to effect a simple "pagniation" of keys.
//
// If the passed 'from' key is empty, Keys will return the first n keys. If the
// passed 'from' key is non-empty, the first key in the returned slice will be
// the key that immediately follows the passed key, in key order.
func (i *LLRBIndex) Keys(from string, n int) <-chan string {
	i.RLock()
	defer i.RUnlock()

	if i.tree == nil || i.less == nil {
		panic("uninitialized index")
	}

	if i.tree.Len() <= 0 {
		// return immediately-closed (empty) chan
		c := make(chan string)
		go close(c)
		return c
	}

	skipFirst := true
	if len(from) <= 0 || !i.tree.Has(from) {
		from = i.tree.Min().(string) // no such key, so start at the top
		skipFirst = false
	}

	c0 := i.tree.IterRange(from, i.tree.Max())
	if skipFirst {
		<-c0
	}

	c := make(chan string)
	go func() {
		wasClosed, sent := false, 0
		for ; sent < n; sent++ {
			key, ok := <-c0
			if !ok {
				wasClosed = true
				break
			}
			c <- key.(string)
		}
		if wasClosed && sent < n {
			// hack to get around IterRange returning only E < @upper
			c <- i.tree.Max().(string)
		}
		close(c)
	}()
	return c
}

//
//
//

// convert converts the Diskv.LessFunction to a format
// usable by the LLRB tree.
func convert(f LessFunction) llrb.LessFunc {
	return func(a, b interface{}) bool {
		aStr, aOk := a.(string)
		bStr, bOk := b.(string)
		if !aOk || !bOk {
			panic("non-string key")
		}

		return f(aStr, bStr)
	}
}

// rebuildIndex does the work of regenerating the index
// with the given keys.
func rebuild(less llrb.LessFunc, keys <-chan string) *llrb.Tree {
	tree := llrb.New(less)
	for key := range keys {
		tree.ReplaceOrInsert(key)
	}
	return tree
}

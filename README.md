# What is diskv?

Diskv (disk-vee) is a simple, persistent key-value store written in the Go
language. It starts with an incredibly simple API for storing arbitrary data on
a filesystem by key, and builds several layers of performance-enhancing
abstraction on top.  The end result is a conceptually simple, but highly
performant, disk-backed storage system.

[![Build Status][1]][2]

[1]: https://secure.travis-ci.org/peterbourgon/diskv.png
[2]: http://www.travis-ci.org/peterbourgon/diskv

# Installing

Install [Go][3], either [from source][4] or [with a prepackaged binary][5]. Be
sure to get at least weekly-2012-03-13 ("Go 1 RC1") or later. Then, run

```
$ go get -v github.com/peterbourgon/diskv
```

[3]: http://weekly.golang.org
[4]: http://weekly.golang.org/doc/install/source
[5]: http://weekly.golang.org/doc/install

# Usage

```
package main

import (
	"fmt"
	"github.com/peterbourgon/diskv"
)

func main() {
	// flatTransform is the simplest possible Transform function,
	// and will put all of the data files into the root directory.
	flatTransform := func(s string) []string { return []string{""} }

	// Initialize a new diskv store, rooted at "my-store-dir".
	// Use flatTransform to place data, and keep a 1MB cache.
	s := diskv.NewStore("my-store-dir", flatTransform, 1024*1024)
	
	// Write three bytes to the key "alpha".
	key := "alpha"
	s.Write(key, []byte{'1', '2', '3'})
	
	// Read the value back out of the store.
	value, _ := s.Read(key)
	fmt.Printf("%v\n", value)
	
	// Erase the key+value from the store (and the disk).
	s.Erase(key)
}
```

More complex examples can be found in the "examples" subdirectory.

# Basic idea

At its core, diskv is a map of a key (`string`) to arbitrary data (`[]byte`).
The data is written to a single file on disk, with the same name as the key.
The key determines where that file will be stored, via a user-provided
`TransformFunc`, which takes a key and returns a slice (`[]string`)
corresponding to a path list where the key file will be stored. The simplest
TransformFunc,

```
func SimpleTransform (key string) []string {
    return []string{""}
}
```

will place all keys in the same, base directory. The design is inspired by
[Redis diskstore][6]; implementing a TransformFunc to emulate the default
diskstore behavior is left as an exercise for the reader.

[6]: http://groups.google.com/group/redis-db/browse_thread/thread/d444bc786689bde9?pli=1

Probably the most important design principle behind diskv is that your data is
always flatly available on the disk. diskv will never do anything that would
prevent you from accessing, copying, backing up, or otherwise interacting with
your data via common UNIX commandline tools.

# Adding a cache

An in-memory caching layer is provided by combining the BasicStore
functionality with a simple map structure, and keeping it up-to-date as
appropriate. Since the map structure in Go is not threadsafe, it's combined
with a RWMutex  to provide safe concurrent access. 

# Adding order

diskv is a key-value store and therefore inherently unordered. An ordering
system can be grafted on by combining the Store functionality with [Petar
Maymounkov's Left-leaning Red-black tree implementation][7]. Basically, diskv
can keep an in-memory index of the keys, ordered by a user-provided LessThan
function. The index is naïvely populated at startup from the keys on-disk, and
kept up-to-date as appropriate.

[7]: https://github.com/petar/GoLLRB 

# Future plans
 
 * Needs plenty of robust testing: huge datasets, etc... 
 * More thorough benchmarking
 * Your suggestions for use-cases I haven't thought of

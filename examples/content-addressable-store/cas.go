package main

import (
	"crypto/md5"
	"fmt"
	"github.com/peterbourgon/diskv"
	"io"
)

const (
	transformBlockSize = 2 // grouping of chars per directory depth
)

func BlockTransform(s string) []string {
	sliceSize := len(s) / transformBlockSize
	pathSlice := make([]string, sliceSize)
	for i := 0; i < sliceSize; i++ {
		from, to := i*transformBlockSize, (i*transformBlockSize)+transformBlockSize
		pathSlice[i] = s[from:to]
	}
	return pathSlice
}

func main() {
	d := diskv.New(diskv.Options{
		BasePath:     "data",
		Transform:    BlockTransform,
		CacheSizeMax: 1024 * 1024, // 1MB
	})

	data := []string{
		"I am the very model of a modern Major-General",
		"I've information vegetable, animal, and mineral",
		"I know the kings of England, and I quote the fights historical",
		"From Marathon to Waterloo, in order categorical",
		"I'm very well acquainted, too, with matters mathematical",
		"I understand equations, both the simple and quadratical",
		"About binomial theorem I'm teeming with a lot o' news",
		"With many cheerful facts about the square of the hypotenuse",
	}
	for _, valueStr := range data {
		key, val := md5sum(valueStr), []byte(valueStr)
		d.Write(key, val)
	}

	keyChan, keyCount := d.Keys(), 0
	for key := range keyChan {
		val, err := d.Read(key)
		if err != nil {
			panic(fmt.Sprintf("key %s had no value", key))
		}
		fmt.Printf("%s: %s\n", key, val)
		keyCount++
	}
	fmt.Printf("%d total keys\n", keyCount)

	// d.EraseAll() // leave it commented out to see how data is kept on disk
}

func md5sum(s string) string {
	h := md5.New()
	io.WriteString(h, s)
	return fmt.Sprintf("%x", h.Sum(nil))
}

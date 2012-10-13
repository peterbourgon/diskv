package main

import (
	"fmt"
	"github.com/peterbourgon/diskv"
)

func main() {
	d := diskv.New(diskv.Options{
		BasePath:     "my-diskv-data-directory",
		Transform:    func(s string) []string { return []string{} },
		CacheSizeMax: 1024 * 1024, // 1MB
	})

	key := "alpha"
	writeErr := d.Write(key, []byte{'1', '2', '3'})
	if writeErr != nil {
		panic(fmt.Sprintf("%s", writeErr))
	}

	value, readErr := d.Read(key)
	if readErr != nil {
		panic(fmt.Sprintf("%s", readErr))
	}
	fmt.Printf("%v\n", value)

	eraseErr := d.Erase(key)
	if eraseErr != nil {
		panic(fmt.Sprintf("%s", eraseErr))
	}
}

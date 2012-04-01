package main

import (
	"fmt"
	"github.com/peterbourgon/diskv"
)

func main() {
	flatTransform := func(s string) []string { return []string{""} }
	s := diskv.NewStore("my-store-dir", flatTransform, 1024*1024)

	key := "alpha"
	writeErr := s.Write(key, []byte{'1', '2', '3'})
	if writeErr != nil {
		panic(fmt.Sprintf("%s", writeErr))
	}

	value, readErr := s.Read(key)
	if readErr != nil {
		panic(fmt.Sprintf("%s", readErr))
	}
	fmt.Printf("%v\n", value)

	eraseErr := s.Erase(key)
	if eraseErr != nil {
		panic(fmt.Sprintf("%s", eraseErr))
	}
}

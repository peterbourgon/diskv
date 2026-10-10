package main

import (
	"fmt"
	"strings"

	"github.com/peterbourgon/diskv/v3"
)

func AdvancedTransformExample(key string) *diskv.PathKey {
	path := strings.Split(key, "/")
	last := len(path) - 1
	return &diskv.PathKey{
		Path:     path[:last],
		FileName: path[last] + ".txt",
	}
}

// If you provide an AdvancedTransform, you must also provide its
// inverse:

func InverseTransformExample(pathKey *diskv.PathKey) (key string) {
	if !strings.HasSuffix(pathKey.FileName, ".txt") {
		return "" // Ignore files not produced by AdvancedTransformExample.
	}
	name := strings.TrimSuffix(pathKey.FileName, ".txt")
	return strings.Join(append(pathKey.Path, name), "/")
}

func main() {
	d := diskv.New(diskv.Options{
		BasePath:          "my-data-dir",
		AdvancedTransform: AdvancedTransformExample,
		InverseTransform:  InverseTransformExample,
		CacheSizeMax:      1024 * 1024,
	})
	// Write some text to the key "alpha/beta/gamma".
	key := "alpha/beta/gamma"
	d.WriteString(key, "¡Hola!") // will be stored in "<basedir>/alpha/beta/gamma.txt"
	fmt.Println(d.ReadString("alpha/beta/gamma"))
}

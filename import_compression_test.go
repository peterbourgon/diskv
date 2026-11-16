package diskv_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/peterbourgon/diskv/v3"
)

func TestImportCompression(t *testing.T) {
	for _, tc := range []struct {
		name        string
		compression diskv.Compression
	}{
		{"gzip", diskv.NewGzipCompression()},
		{"zlib", diskv.NewZlibCompression()},
		{"zlib-dict", diskv.NewZlibCompressionLevelDict(-1, []byte("imported value"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, move := range []bool{false, true} {
				name := "copy"
				if move {
					name = "move"
				}
				t.Run(name, func(t *testing.T) {
					for _, overwrite := range []bool{false, true} {
						name := "new"
						if overwrite {
							name = "overwrite-cached"
						}
						t.Run(name, func(t *testing.T) {
							dir, err := ioutil.TempDir("", "diskv-import-compression-")
							if err != nil {
								t.Fatal(err)
							}
							defer os.RemoveAll(dir)
							d := diskv.New(diskv.Options{
								BasePath:     filepath.Join(dir, "store"),
								Compression:  tc.compression,
								CacheSizeMax: 1024,
							})
							if overwrite {
								d.TempDir = filepath.Join(dir, "temp")
								if err := d.Write("key", []byte("old value")); err != nil {
									t.Fatal(err)
								}
								if _, err := d.Read("key"); err != nil {
									t.Fatal(err)
								}
							}
							value := []byte("imported value, not already compressed")
							source := filepath.Join(dir, "source")
							if err := ioutil.WriteFile(source, value, 0600); err != nil {
								t.Fatal(err)
							}
							if err := d.Import(source, "key", move); err != nil {
								t.Fatal(err)
							}
							got, err := d.Read("key")
							if err != nil {
								t.Fatalf("read imported value: %v", err)
							}
							if !bytes.Equal(got, value) {
								t.Fatalf("read %q, want %q", got, value)
							}
							// Check a subsequent read after the value has been cached.
							got, err = d.Read("key")
							if err != nil || !bytes.Equal(got, value) {
								t.Fatalf("second read = %q, %v", got, err)
							}
							if _, err := os.Stat(source); move {
								if !os.IsNotExist(err) {
									t.Fatalf("moved source still exists: %v", err)
								}
							} else if err != nil {
								t.Fatalf("copied source missing: %v", err)
							}
						})
					}
				})
			}
		})
	}
}

func TestImportMoveCompressionFailureKeepsSource(t *testing.T) {
	dir, err := ioutil.TempDir("", "diskv-import-compression-error-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	source := filepath.Join(dir, "source")
	value := []byte("source must survive a failed compression")
	if err := ioutil.WriteFile(source, value, 0600); err != nil {
		t.Fatal(err)
	}
	d := diskv.New(diskv.Options{
		BasePath:    filepath.Join(dir, "store"),
		Compression: diskv.NewGzipCompressionLevel(100), // invalid level
	})
	if err := d.Import(source, "key", true); err == nil {
		t.Error("expected compression error")
	}
	got, err := ioutil.ReadFile(source)
	if err != nil || !bytes.Equal(got, value) {
		t.Fatalf("source = %q, %v", got, err)
	}
	if d.Has("key") {
		t.Error("failed import left a destination key")
	}
}

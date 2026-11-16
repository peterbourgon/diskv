package diskv_test

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/peterbourgon/diskv/v3"
)

func TestImportMoveCompressedSameFile(t *testing.T) {
	for _, atomic := range []bool{false, true} {
		mode := "direct"
		if atomic {
			mode = "temp-dir"
		}
		t.Run(mode, func(t *testing.T) {
			for _, alias := range []string{"exact", "dot-path", "hard-link", "directory-symlink"} {
				t.Run(alias, func(t *testing.T) {
					dir, err := ioutil.TempDir("", "diskv-import-samefile-")
					if err != nil {
						t.Fatal(err)
					}
					defer os.RemoveAll(dir)
					d := diskv.New(diskv.Options{
						BasePath:     filepath.Join(dir, "store"),
						Compression:  diskv.NewGzipCompression(),
						CacheSizeMax: 1024,
					})
					if atomic {
						d.TempDir = filepath.Join(dir, "temp")
					}
					value := []byte("already stored and compressed")
					if err := d.Write("key", value); err != nil {
						t.Fatal(err)
					}
					if _, err := d.Read("key"); err != nil {
						t.Fatal(err)
					}
					target := filepath.Join(d.BasePath, "key")
					source := target
					switch alias {
					case "dot-path":
						source = d.BasePath + string(os.PathSeparator) + "." + string(os.PathSeparator) + "key"
					case "hard-link":
						source = filepath.Join(dir, "link")
						if err := os.Link(target, source); err != nil {
							t.Fatal(err)
						}
					case "directory-symlink":
						link := filepath.Join(dir, "alias")
						if err := os.Symlink(d.BasePath, link); err != nil {
							t.Skipf("directory symlinks unavailable: %v", err)
						}
						source = filepath.Join(link, "key")
					}
					before, err := ioutil.ReadFile(target)
					if err != nil {
						t.Fatal(err)
					}
					info, err := os.Stat(target)
					if err != nil {
						t.Fatal(err)
					}
					if err := d.Import(source, "key", true); err != nil {
						t.Fatal(err)
					}
					// Renaming a file onto itself (including a hard link) is a no-op.
					// Neither name may be removed, nor the data compressed a second time.
					for _, path := range []string{target, source} {
						got, err := ioutil.ReadFile(path)
						if err != nil || !bytes.Equal(got, before) {
							t.Errorf("stored bytes changed at %q: got %q, error %v", path, got, err)
						}
						gotInfo, err := os.Stat(path)
						if err != nil || !os.SameFile(info, gotInfo) {
							t.Errorf("original file replaced at %q: %v", path, err)
						}
					}
					got, err := d.Read("key")
					if err != nil || !bytes.Equal(got, value) {
						t.Errorf("read = %q, %v; want %q", got, err, value)
					}
				})
			}
		})
	}
}

func TestImportCopyCompressedSameFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "diskv-import-copy-samefile-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	d := diskv.New(diskv.Options{
		BasePath:    filepath.Join(dir, "store"),
		TempDir:     filepath.Join(dir, "temp"),
		Compression: diskv.NewGzipCompression(),
	})
	if err := d.Write("key", []byte("stored value")); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(d.BasePath, "key")
	before, err := ioutil.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	// Copy imports still treat the source bytes as a new value, even when
	// the source is an existing compressed file in the store.
	if err := d.Import(source, "key", false); err != nil {
		t.Fatal(err)
	}
	got, err := d.Read("key")
	if err != nil || !bytes.Equal(got, before) {
		t.Fatalf("read = %q, %v; want source bytes %q", got, err, before)
	}
}

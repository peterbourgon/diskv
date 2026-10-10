package diskv

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestWalkerSkipsDirectoriesBeforeInverseTransform(t *testing.T) {
	dir, err := ioutil.TempDir("", "diskv-inverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	nested := filepath.Join(dir, "nested")
	if err := os.Mkdir(nested, 0700); err != nil {
		t.Fatal(err)
	}
	calls := 0
	d := New(Options{BasePath: dir, InverseTransform: func(p *PathKey) string { calls++; return p.FileName }})
	for _, path := range []string{dir, nested} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.walker(make(chan string, 1), "", nil)(path, info, nil); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 0 {
		t.Fatalf("inverse transform called %d times for directories", calls)
	}
}

func TestKeysAdvancedTransformSkipsInvalidFiles(t *testing.T) {
	dir, err := ioutil.TempDir("", "diskv-inverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	var calls []string
	d := New(Options{BasePath: dir, AdvancedTransform: func(key string) *PathKey {
		parts := strings.Split(key, "/")
		return &PathKey{Path: parts[:len(parts)-1], FileName: parts[len(parts)-1] + ".txt"}
	}, InverseTransform: func(p *PathKey) string {
		calls = append(calls, p.FileName)
		if !strings.HasSuffix(p.FileName, ".txt") {
			return ""
		}
		return strings.Join(append(p.Path, strings.TrimSuffix(p.FileName, ".txt")), "/")
	}})
	for _, key := range []string{"root", "alpha/one", "alpha/deep/two", "beta/other"} {
		if err := d.WriteString(key, key); err != nil {
			t.Fatal(err)
		}
	}
	if err := ioutil.WriteFile(filepath.Join(dir, "stray"), []byte("not a value"), 0600); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for key := range d.Keys(nil) {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{"alpha/deep/two", "alpha/one", "beta/other", "root"}
	if !reflect.DeepEqual(keys, want) {
		t.Errorf("keys=%q, want %q", keys, want)
	}
	for _, name := range calls {
		if name != "stray" && !strings.HasSuffix(name, ".txt") {
			t.Errorf("directory passed to inverse transform: %q", name)
		}
	}
	keys = nil
	for key := range d.KeysPrefix("alpha/", nil) {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if !reflect.DeepEqual(keys, want[:2]) {
		t.Errorf("prefix keys=%q, want %q", keys, want[:2])
	}
}

func TestWalkerPreservesFileErrorsPrefixAndCancellation(t *testing.T) {
	dir, err := ioutil.TempDir("", "diskv-inverse-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "key")
	if err := ioutil.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	d := New(Options{BasePath: dir, InverseTransform: func(p *PathKey) string { calls++; return p.FileName }})
	out := make(chan string, 1)
	failure := errors.New("walk failure")
	if got := d.walker(out, "", nil)(path, nil, failure); got != failure || calls != 0 {
		t.Fatalf("error=%v, calls=%d", got, calls)
	}
	if err := d.walker(out, "other", nil)(path, info, nil); err != nil || len(out) != 0 {
		t.Fatalf("prefix error=%v, output=%d", err, len(out))
	}
	if err := d.walker(out, "ke", nil)(path, info, nil); err != nil {
		t.Fatal(err)
	}
	if key := <-out; key != "key" {
		t.Fatalf("key=%q", key)
	}
	cancel := make(chan struct{})
	close(cancel)
	if err := d.walker(make(chan string), "", cancel)(path, info, nil); err != errCanceled {
		t.Fatalf("cancellation=%v", err)
	}
}

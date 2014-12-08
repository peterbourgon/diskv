package diskv_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/peterbourgon/diskv"
)

var (
	keysTestData = map[string]string{
		"ab01cd01": "When we started building CoreOS",
		"ab01cd02": "we looked at all the various components available to us",
		"ab01cd03": "re-using the best tools",
		"ef01gh04": "and building the ones that did not exist",
		"ef02gh05": "We believe strongly in the Unix philosophy",
		"xxxxxxxx": "tools should be independently useful",
	}
	prefixes = []string{
		"a",
		"ab",
		"ab0",
		"ab01",
		"ab01cd0",
		"ab01cd01",
		"ab01cd01x", // none
		"b",         // none
		"b0",        // none
		"0",         // none
		"01",        // none
		"e",
		"ef",
		"efx", // none
		"ef01gh0",
		"ef01gh04",
		"ef01gh05",
		"ef01gh06", // none
	}
)

func TestKeysFlat(t *testing.T) {
	d := diskv.New(diskv.Options{
		BasePath: "test-data",
	})
	defer d.EraseAll()

	for k, v := range keysTestData {
		d.Write(k, []byte(v))
	}

	checkKeys(t, d.Keys(), keysTestData)
}

func TestKeysNested(t *testing.T) {
	d := diskv.New(diskv.Options{
		BasePath:  "test-data",
		Transform: blockTransform(2),
	})
	defer d.EraseAll()

	for k, v := range keysTestData {
		d.Write(k, []byte(v))
	}

	checkKeys(t, d.Keys(), keysTestData)
}

func TestKeysPrefixFlat(t *testing.T) {
	d := diskv.New(diskv.Options{
		BasePath: "test-data",
	})
	defer d.EraseAll()

	for k, v := range keysTestData {
		d.Write(k, []byte(v))
	}

	for _, prefix := range prefixes {
		checkKeys(t, d.KeysPrefix(prefix), filterPrefix(keysTestData, prefix))
	}
}

func TestKeysPrefixNested(t *testing.T) {
	d := diskv.New(diskv.Options{
		BasePath:  "test-data",
		Transform: blockTransform(2),
	})
	defer d.EraseAll()

	for k, v := range keysTestData {
		d.Write(k, []byte(v))
	}

	for _, prefix := range prefixes {
		checkKeys(t, d.KeysPrefix(prefix), filterPrefix(keysTestData, prefix))
	}
}

func checkKeys(t *testing.T, c <-chan string, want map[string]string) {
	for k := range c {
		if _, ok := want[k]; !ok {
			t.Errorf("%q yielded but not expected", k)
			continue
		}

		delete(want, k)
		t.Logf("%q yielded OK", k)
	}

	if len(want) != 0 {
		t.Errorf("%d expected key(s) not yielded: %s", len(want), strings.Join(flattenKeys(want), ", "))
	}
}

func blockTransform(blockSize int) func(string) []string {
	return func(s string) []string {
		var (
			sliceSize = len(s) / blockSize
			pathSlice = make([]string, sliceSize)
		)
		for i := 0; i < sliceSize; i++ {
			from, to := i*blockSize, (i*blockSize)+blockSize
			pathSlice[i] = s[from:to]
		}
		return pathSlice
	}
}

func filterPrefix(in map[string]string, prefix string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		if strings.HasPrefix(k, prefix) {
			out[k] = v
		}
	}
	return out
}

func TestFilterPrefix(t *testing.T) {
	input := map[string]string{
		"all":        "",
		"and":        "",
		"at":         "",
		"available":  "",
		"best":       "",
		"building":   "",
		"components": "",
		"coreos":     "",
		"did":        "",
		"exist":      "",
		"looked":     "",
		"not":        "",
		"ones":       "",
		"re-using":   "",
		"started":    "",
		"that":       "",
		"the":        "",
		"to":         "",
		"tools":      "",
		"us":         "",
		"various":    "",
		"we":         "",
		"when":       "",
	}

	for prefix, want := range map[string]map[string]string{
		"a":    map[string]string{"all": "", "and": "", "at": "", "available": ""},
		"al":   map[string]string{"all": ""},
		"all":  map[string]string{"all": ""},
		"alll": map[string]string{},
		"c":    map[string]string{"components": "", "coreos": ""},
		"co":   map[string]string{"components": "", "coreos": ""},
		"com":  map[string]string{"components": ""},
	} {
		have := filterPrefix(input, prefix)
		if !reflect.DeepEqual(want, have) {
			t.Errorf("%q: want %v, have %v", prefix, flattenKeys(want), flattenKeys(have))
		}
	}
}

func flattenKeys(m map[string]string) []string {
	a := make([]string, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	return a
}

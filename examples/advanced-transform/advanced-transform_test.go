package main

import (
	"github.com/peterbourgon/diskv/v3"
	"testing"
)

func TestAdvancedTransformExampleRoundtrip(t *testing.T) {
	for _, key := range []string{"root", "alpha/beta/gamma", "ユニコード/🙂"} {
		if got := InverseTransformExample(AdvancedTransformExample(key)); got != key {
			t.Errorf("key=%q, got=%q", key, got)
		}
	}
}

func TestInverseTransformExampleIgnoresInvalidFiles(t *testing.T) {
	for _, name := range []string{"", ".", "a", "wrong.bin"} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if v := recover(); v != nil {
					t.Fatalf("invalid filename caused panic: %v", v)
				}
			}()
			if key := InverseTransformExample(&diskv.PathKey{FileName: name}); key != "" {
				t.Errorf("invalid filename became key %q", key)
			}
		})
	}
}

package dashtable

import (
	"fmt"
	"testing"
)

func TestDashtable(t *testing.T) {
	noOfItems := 100_000
	dtb := New[int, string](uint64(noOfItems))
	for i := 0; i < noOfItems; i++ {
		dtb.Set(i, fmt.Sprintf("value#%d", i))
	}

	for i := 0; i < noOfItems; i++ {
		ok, v := dtb.Get(int(i))
		if !ok {
			t.Errorf("Get(%d) got false, expected found item", i)
			return
		}
		if v != fmt.Sprintf("value#%d", i) {
			t.Errorf("Get(%d) got %q, expected valid value", i, v)
			return
		}
	}

	for i := noOfItems; i < noOfItems*2; i++ {
		ok, v := dtb.Get(i)
		if ok {
			t.Errorf("Get(%d) got true, expected not found item", i)
			return
		}
		if v != "" {
			t.Errorf("Get(%d) got %q, expected empty value", i, v)
			return
		}
	}
}

func BenchmarkDashtable_Set(b *testing.B) {
	b.StopTimer()
	noOfItems := 100_000
	dtb := New[int, string](uint64(noOfItems))
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		dtb.Set(i%noOfItems, "benchmark")
	}
}

var (
	_ok    bool
	_value string
)

func BenchmarkDashtable_Get(b *testing.B) {
	b.StopTimer()
	noOfItems := 100_000
	dtb := New[int, string](uint64(noOfItems))
	for i := 0; i < noOfItems; i++ {
		dtb.Set(i%noOfItems, "benchmark")
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_ok, _value = dtb.Get(i % noOfItems)
	}
}

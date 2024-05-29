package hummingbird

import (
	"runtime"
	"sync"
	"testing"
)

func BenchmarkMap_Set(b *testing.B) {
	b.StopTimer()
	noOfItems := 100_000
	m := NewMap[int, string](uint64(runtime.GOMAXPROCS(0)), uint64(noOfItems))
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		m.Set(i%noOfItems, "benchmark")
	}
}

var (
	_ok    bool
	_value string
)

func BenchmarkMap_Get(b *testing.B) {
	b.StopTimer()
	noOfItems := 100_000
	m := NewMap[int, string](uint64(runtime.GOMAXPROCS(0)), uint64(noOfItems))
	for i := 0; i < noOfItems; i++ {
		m.Set(i, "benchmark")
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_ok, _value = m.Get(i % noOfItems)
	}
}

var _syncMapValue any

func BenchmarkGoSyncMap(b *testing.B) {
	b.StopTimer()
	var m sync.Map
	noOfItems := 100_000
	for i := 0; i < noOfItems; i++ {
		m.Store(i, "benchmark")
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_syncMapValue, _ok = m.Load(i % noOfItems)
	}
}

package hummingbird

import (
	"runtime"
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

package dashtable

import (
	"fmt"
	"hash/maphash"
	"log"
	"math"
)

// Dashtable is a toy implementation of dragonfly's dashtable.
// https://github.com/dragonflydb/dragonfly/blob/main/docs/dashtable.md
// A dashtable is expected to be run in a single thread/goroutine only, and currently
// it's NOT thread-safe to call Set and Get in parallel.
// To better scale out, build the application top of the Dashtable which pin each CPU thread to
// a dashtable instance.
type Dashtable[K comparable, V any] struct {
	hash maphash.Hash

	segments []segment[K, V] // Segment Directory
}

// New returns a ready to use Dashtable.
//
// noMaxItem provides a hint on how many segments we need to create first hand to be able to hold up to
// that maximum number of items,
// as we haven't implemented dashtable growth via segment splitting yet.
func New[K comparable, V any](noMaxItem uint64) Dashtable[K, V] {
	// Each segment can hold up to 840 items. x300 the size to prevent segment full and reaching panic case.
	// TODO: Remove this after implementing segment splitting, the memory usage should be reduced by 300x.
	noOfSegments := noMaxItem / 840 * 300
	if noOfSegments < 12 {
		noOfSegments = 12
	}

	dtb := Dashtable[K, V]{
		hash:     maphash.Hash{},
		segments: make([]segment[K, V], noOfSegments), // Escape to heap
	}
	dtb.hash.SetSeed(maphash.MakeSeed())
	for i := range dtb.segments {
		dtb.segments[i] = newSegment[K, V]()
	}
	return dtb
}

func (dtb *Dashtable[K, V]) Set(key K, value V) {
	keySum := dtb.Sum(key)
	segmentIdx := keySum % uint64(len(dtb.segments))
	homeSegment := &dtb.segments[segmentIdx]
	if isSet := homeSegment.set(keySum, key, value); isSet {
		return // Happy case
	}

	// TODO: Segment is full. Handle segment split, currently just panic here
	log.Panicf("failed to set key=%v as dashtable's segment is full", key)
}

func (dtb *Dashtable[K, V]) Get(key K) (ok bool, value V) {
	keySum := dtb.Sum(key)
	segmentIdx := keySum % uint64(len(dtb.segments))
	homeSegment := &dtb.segments[segmentIdx]
	return homeSegment.get(keySum, key)
}

func (dtb *Dashtable[K, V]) Sum(key K) uint64 {
	keyBytes := getBytes(key)
	_, _ = dtb.hash.Write(keyBytes)
	keySum := dtb.hash.Sum64()
	dtb.hash.Reset()
	return keySum
}

func getBytes(key any) (b []byte) {
	switch v := key.(type) {
	case byte:
		return []byte{v}
	case uint32:
		b = make([]byte, 8)
		littleEndianPutUint64(b, uint64(v)) // TODO
		return b
	case int32:
		b = make([]byte, 8)
		littleEndianPutUint64(b, uint64(v)) // TODO
		return b
	case uint64:
		b = make([]byte, 8)
		littleEndianPutUint64(b, v)
		return b
	case int64:
		b = make([]byte, 8)
		littleEndianPutUint64(b, uint64(v)) // TODO
		return b
	case int:
		b = make([]byte, 8)
		littleEndianPutUint64(b, uint64(v)) // TODO
		return b
	case float32:
		b = make([]byte, 8)
		uint64Bits := math.Float64bits(float64(v)) // TODO
		littleEndianPutUint64(b, uint64Bits)
		return b
	case float64:
		b = make([]byte, 8)
		uint64Bits := math.Float64bits(v)
		littleEndianPutUint64(b, uint64Bits)
		return b
	case string:
		return StringToBytes(v)
	default:
		return StringToBytes(fmt.Sprintf("%v", v))
	}
}

func littleEndianPutUint64(b []byte, v uint64) {
	_ = b[7] // early bounds check to guarantee safety of writes below
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
	b[5] = byte(v >> 40)
	b[6] = byte(v >> 48)
	b[7] = byte(v >> 56)
}

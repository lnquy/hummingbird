package dashtable

import (
	"fmt"
	"hash/maphash"
	"log"
)

// Dashtable is a toy implementation of dragonfly's dashtable.
// https://github.com/dragonflydb/dragonfly/blob/main/docs/dashtable.md
// A dashtable is expected to be run in a single thread/goroutine only, and currently
// it's NOT thread-safe to call Set and Get in parallel.
// To better scale out, build the application top of the Dashtable which pin each CPU thread to
// a dashtable instance.
type Dashtable[K comparable, V any] struct {
	hash maphash.Hash

	segments []*segment[K, V] // Segment Directory
}

// New returns a ready to use Dashtable.
//
// noMaxItem provides a hint on how many segments we need to create first hand to be able to hold up to
// that maximum number of items,
// as we haven't implemented dashtable growth via segment splitting yet.
func New[K comparable, V any](noMaxItem uint64) *Dashtable[K, V] {
	// Each segment can hold up to 840 items. x300 the size to prevent segment full and reaching panic case.
	// TODO: Remove this after implementing segment splitting, the memory usage should be reduced by 300x.
	noOfSegments := noMaxItem / 840 * 300

	dtb := &Dashtable[K, V]{
		hash:     maphash.Hash{},
		segments: make([]*segment[K, V], noOfSegments),
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
	homeSegment := dtb.segments[segmentIdx]
	if isSet := homeSegment.set(keySum, key, value); isSet {
		return // Happy case
	}

	// TODO: Segment is full. Handle segment split, currently just panic here
	log.Panicf("failed to set key=%v as dashtable's segment is full", key)
}

func (dtb *Dashtable[K, V]) Get(key K) (ok bool, value V) {
	keySum := dtb.Sum(key)
	segmentIdx := keySum % uint64(len(dtb.segments))
	homeSegment := dtb.segments[segmentIdx]
	return homeSegment.get(keySum, key)
}

func (dtb *Dashtable[K, V]) Sum(key K) uint64 {
	keyBytes := StringToBytes(fmt.Sprintf("%v", key)) // TODO: Slow fmt.Sprintf
	_, _ = dtb.hash.Write(keyBytes)
	keySum := dtb.hash.Sum64()
	dtb.hash.Reset()
	return keySum
}

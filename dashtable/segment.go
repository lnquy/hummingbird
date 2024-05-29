package dashtable

type segment[K comparable, V any] struct {
	// 0-56: Regular buckets
	// 56:60: Stash buckets
	buckets [60]*bucket[K, V]
}

func newSegment[K comparable, V any]() *segment[K, V] {
	s := &segment[K, V]{
		buckets: [60]*bucket[K, V]{},
	}
	for i := range s.buckets {
		s.buckets[i] = newBucket[K, V]()
	}
	return s
}

func (s *segment[K, V]) set(keySum uint64, key K, value V) (isSet bool) {
	bucketIdx := keySum % 56 // First 56 buckets are the regular ones
	bkt := s.buckets[bucketIdx]
	if isSetAtHomeBucket := bkt.set(key, value); isSetAtHomeBucket {
		// Happy case, can put this item to its home bucket
		return true
	}

	// Otherwise, check the neighbor bucket for empty room
	bucketIdx++
	if bucketIdx < 56 {
		if isSetAtNeighborBucket := s.buckets[bucketIdx].set(key, value); isSetAtNeighborBucket {
			// 2nd happy case, can put this item to its neighbor bucket
			return
		}
	}

	// Otherwise, try to put this item in the stash buckets
	for stashBucketIdx := 56; stashBucketIdx < 60; stashBucketIdx++ {
		if isSetAtStashBucket := s.buckets[stashBucketIdx].set(key, value); isSetAtStashBucket {
			// 3rd happy case, can put this item to the stash bucket
			return
		}
	}

	// Segment is full, couldn't put the current item in this segment as it's full.
	// Will need to handle segment split in this case.
	return false
}

func (s *segment[K, V]) get(keySum uint64, key K) (ok bool, value V) {
	bucketIdx := keySum % 56 // First 56 buckets are the regular ones
	homeBucket := s.buckets[bucketIdx]
	if isFoundAtHomeBucket, value := homeBucket.get(key); isFoundAtHomeBucket {
		// Happy case, found the item in its home bucket
		return true, value
	}

	bucketIdx++
	if bucketIdx < 56 {
		if isFoundAtNeighborBucket, value := s.buckets[bucketIdx].get(key); isFoundAtNeighborBucket {
			// 2nd happy case, can put this item to its neighbor bucket
			return true, value
		}
	}

	// Otherwise, try to find this item in the stash buckets
	for stashBucketIdx := 56; stashBucketIdx < 60; stashBucketIdx++ {
		if isFoundAtStashBucket, value := s.buckets[stashBucketIdx].get(key); isFoundAtStashBucket {
			// 3rd happy case, can put this item to the stash bucket
			return true, value
		}
	}

	// Not found
	return false, value
}

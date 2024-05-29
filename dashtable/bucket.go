package dashtable

type bucket[K comparable, V any] struct {
	slots [14]*slot[K, V]
}

func newBucket[K comparable, V any]() *bucket[K, V] {
	return &bucket[K, V]{
		slots: [14]*slot[K, V]{},
	}
}

func (b *bucket[K, V]) set(key K, value V) (isSet bool) {
	for i, slt := range b.slots {
		if slt == nil { // Found empty slot, set it
			b.slots[i] = &slot[K, V]{
				key:   key,
				value: value,
			}
			return true
		}
		if slt.match(key) { // Found an existing key, override the value
			b.slots[i].value = value
			return true
		}
	}

	// Reached the end of the bucket, no room left to add this item
	return false
}

func (b *bucket[K, V]) get(key K) (ok bool, value V) {
	for _, slt := range b.slots {
		if slt == nil { // Reached the end of the bucket
			return false, value
		}
		if slt.match(key) {
			return true, slt.value
		}
	}
	return false, value
}

package dashtable

type bucket[K comparable, V any] struct {
	length byte
	slots  [14]slot[K, V]
}

func newBucket[K comparable, V any]() bucket[K, V] {
	return bucket[K, V]{
		slots: [14]slot[K, V]{},
	}
}

func (b *bucket[K, V]) set(key K, value V) (isSet bool) {
	for i := byte(0); i < b.length; i++ {
		slt := b.slots[i]
		if slt.match(key) { // Found an existing key, override the value
			b.slots[i].value = value
			return true
		}
	}
	if int(b.length) < len(b.slots) {
		b.slots[b.length] = slot[K, V]{
			key:   key,
			value: value,
		}
		b.length++
		return true
	}

	// Reached the end of the bucket, no room left to add this item
	return false
}

func (b *bucket[K, V]) get(key K) (ok bool, value V) {
	for i := byte(0); i < b.length; i++ {
		slt := b.slots[i]
		if slt.match(key) {
			return true, slt.value
		}
	}
	return false, value
}

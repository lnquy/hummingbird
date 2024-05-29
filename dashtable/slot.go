package dashtable

type slot[K comparable, V any] struct {
	key   K
	value V
}

func (s slot[K, V]) match(key K) (ok bool) {
	return s.key == key
}

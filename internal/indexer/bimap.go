package indexer

import set "github.com/deckarep/golang-set/v2"

type BiMap[K comparable, V comparable] struct {
	forward  map[K]set.Set[V]
	backward map[V]set.Set[K]
}

func NewBiMap[K comparable, V comparable]() *BiMap[K, V] {
	return &BiMap[K, V]{
		forward:  make(map[K]set.Set[V]),
		backward: make(map[V]set.Set[K]),
	}
}

func (b *BiMap[K, V]) Add(k K, v V) {
	if _, ok := b.forward[k]; !ok {
		b.forward[k] = set.NewSet[V]()
	}
	b.forward[k].Add(v)

	if _, ok := b.backward[v]; !ok {
		b.backward[v] = set.NewSet[K]()
	}
	b.backward[v].Add(k)
}

func (b *BiMap[K, V]) GetForward(k K) set.Set[V] {
	if set, ok := b.forward[k]; ok {
		return set
	}
	return set.NewSet[V]()
}

func (b *BiMap[K, V]) GetBackward(v V) set.Set[K] {
	if set, ok := b.backward[v]; ok {
		return set
	}
	return set.NewSet[K]()
}

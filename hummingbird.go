package hummingbird

import (
	"context"
	"fmt"
	"hash/maphash"

	"github.com/lnquy/hummingbird/dashtable"
)

type Map[K comparable, V any] struct {
	ctx       context.Context
	ctxCancel context.CancelFunc

	hash            maphash.Hash
	noOfThreads     uint64
	commandChannels []chan *command[K, V]
}

func NewMap[K comparable, V any](noOfThreads, maxNoOfItems uint64) *Map[K, V] {
	ctx, ctxCancel := context.WithCancel(context.Background())
	m := &Map[K, V]{
		ctx:             ctx,
		ctxCancel:       ctxCancel,
		hash:            maphash.Hash{},
		noOfThreads:     noOfThreads,
		commandChannels: make([]chan *command[K, V], noOfThreads),
	}
	m.hash.SetSeed(maphash.MakeSeed())

	// TODO: Actually each thread should be able to receive the command by it own then forward the command
	// to the correct peer thread, instead of relying on a centralized coordinator thread like this;
	// which could be a bottleneck.
	for i := 0; i < int(noOfThreads); i++ {
		commandChan := make(chan *command[K, V], 100)
		workerThread := newThread[K, V](fmt.Sprintf("%d", i), commandChan, maxNoOfItems/noOfThreads)
		go workerThread.handle(m.ctx)
		m.commandChannels[i] = commandChan
	}
	return m
}

func (m *Map[K, V]) Set(key K, value V) {
	keyBytes := dashtable.StringToBytes(fmt.Sprintf("%v", key)) // TODO: Slow fmt.Sprintf
	_, _ = m.hash.Write(keyBytes)
	keySum := m.hash.Sum64()
	m.hash.Reset()

	cmdChan := m.commandChannels[keySum%m.noOfThreads]
	cmdChan <- &command[K, V]{
		cmd:           cmdSET,
		key:           key,
		value:         value,
		getResultChan: nil,
	}
}

func (m *Map[K, V]) Get(key K) (ok bool, value V) {
	keyBytes := dashtable.StringToBytes(fmt.Sprintf("%v", key)) // TODO: Slow fmt.Sprintf
	_, _ = m.hash.Write(keyBytes)
	keySum := m.hash.Sum64()
	m.hash.Reset()

	resultChan := make(chan *getResult[V], 1)
	cmdChan := m.commandChannels[keySum%m.noOfThreads]
	cmdChan <- &command[K, V]{
		cmd:           cmdGET,
		key:           key,
		getResultChan: resultChan,
	}
	result := <-resultChan
	return result.found, result.value
}

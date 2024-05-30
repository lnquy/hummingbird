package hummingbird

import (
	"context"
	"log"

	"github.com/lnquy/hummingbird/dashtable"
)

const (
	cmdSET cmdType = iota
	cmdGET
)

type cmdType uint64

type command[K comparable, V any] struct {
	cmd           cmdType
	key           K
	value         V
	getResultChan chan<- *getResult[V]
}

type getResult[V any] struct {
	found bool
	value V
}

type thread[K comparable, V any] struct {
	name        string
	dtb         *dashtable.Dashtable[K, V]
	commandChan <-chan *command[K, V]
}

func newThread[K comparable, V any](name string, commandChan <-chan *command[K, V], noOfItemInShard uint64) *thread[K, V] {
	dtb := dashtable.New[K, V](noOfItemInShard)
	return &thread[K, V]{
		name:        name,
		dtb:         &dtb,
		commandChan: commandChan,
	}
}

func (t *thread[K, V]) handle(ctx context.Context) {
	// log.Printf("thread#%s: started", t.name)
	for {
		select {
		case <-ctx.Done():
			// log.Printf("thread#%s: context cancel received, exiting", t.name)
			return
		case cmd := <-t.commandChan:
			switch cmd.cmd {
			case cmdSET:
				t.dtb.Set(cmd.key, cmd.value)
			case cmdGET:
				ok, v := t.dtb.Get(cmd.key)
				cmd.getResultChan <- &getResult[V]{
					found: ok,
					value: v,
				}
			default:
				log.Panicf("unsupported command type: %d", cmd.cmd)
			}
		}
	}
}

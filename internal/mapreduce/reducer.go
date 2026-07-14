package mapreduce

import (
	"context"
	"fmt"
	"sync"

	"github.com/prutwee/semnautics/pkg/types"
)

// this struct will be replaced by an Apache Arrow memory buffer.
type PartitionState struct {
	State map[uint64]float64
}

type ReducerPool struct {
	partitions []<-chan types.MappedRecord
	states     []*PartitionState
}

func NewReducerPool(shufflerPartitions []<-chan types.MappedRecord) *ReducerPool {
	numPartitions := len(shufflerPartitions)
	states := make([]*PartitionState, numPartitions)

	for i := 0; i < numPartitions; i++ {
		// Pre-allocate map capacity to avoid expensive resizing during traffic spikes.
		// A capacity of 10,000 is a safe starting heuristic for L1/L2 CPU caches.
		states[i] = &PartitionState{
			State: make(map[uint64]float64, 10000),
		}
	}

	return &ReducerPool{
		partitions: shufflerPartitions,
		states:     states,
	}
}

// Start spins up exactly one goroutine per partition channel.
func (r *ReducerPool) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for i, ch := range r.partitions {
		wg.Add(1)
		// We pass the index 'i' and the channel 'ch' into the closure
		// to avoid the classic Go loop variable capture bug.
		go func(partitionID int, inbound <-chan types.MappedRecord) {
			defer wg.Done()
			r.worker(ctx, partitionID, inbound)
		}(i, ch)
	}

	go func() {
		wg.Wait()
		fmt.Println("All Reducers have successfully drained and shut down.")
	}()
}

func (r *ReducerPool) worker(ctx context.Context, id int, inbound <-chan types.MappedRecord) {
	localState := r.states[id].State

	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-inbound:
			if !ok {
				return
			}

			// Apply the mathematical delta based on the CDC operation type.
			// Because there are no locks, this executes in nanoseconds.
			switch record.Operation {
			case types.OpInsert, types.OpUpdate:
				localState[record.DimensionHash] += record.Value
			case types.OpDelete:
				// Retraction logic: When a database row is deleted, we cleanly
				// remove its value from the aggregate state.
				localState[record.DimensionHash] -= record.Value
			}
		}
	}
}

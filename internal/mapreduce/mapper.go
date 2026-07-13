package mapreduce

import (
	"context"
	"sync"

	"github.com/prutwee/semnautics/pkg/types"
)

type MapFunc func(event types.Event) ([]types.MappedRecord, error)

type MapperPool struct {
	numWorkers int
	mapFn      MapFunc
}

// initializes the pool, numWorkers should align with runtime.NumCPU() or lesser if req
func NewMapperPool(numWorkers int, mapFn MapFunc) *MapperPool {
	return &MapperPool{
		numWorkers: numWorkers,
		mapFn:      mapFn,
	}
}

func (p *MapperPool) Start(
	ctx context.Context,
	in <-chan types.Event,
	out chan<- types.MappedRecord) { // spins up the workers

	var wg sync.WaitGroup

	for i := 0; i < p.numWorkers; i++ { // (0,1,2,3)
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.worker(ctx, in, out)
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
}

func (p *MapperPool) worker(
	ctx context.Context,
	in <-chan types.Event,
	out chan<- types.MappedRecord) {

	for {
		select {
		case <-ctx.Done():
			// A shutdown signal was received (e.g., SIGTERM). Exit cleanly.
			return
		case event, ok := <-in:
			if !ok {
				// The ingestion source closed the 'in' channel. No more data.
				return
			}

			// Execute the dynamic mapping logic injected by the DAG.
			records, err := p.mapFn(event)
			if err != nil {
				// TODO : this should route to a DL Queue
				continue
			}

			// Push mapped records downstream to the Shuffle phase.
			for _, rec := range records {
				select {
				case <-ctx.Done():
					return
				case out <- rec:
				}
			}
		}
	}
}

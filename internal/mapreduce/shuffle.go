package mapreduce

import (
	"context"
	"fmt"
	"math/bits"

	"github.com/prutwee/semnautics/pkg/types"
)

type Shuffler struct {
	partitions uint64
	mask       uint64
	outbound   []chan types.MappedRecord
}

func NewShuffler(partitions int, channelBufferSize int) (*Shuffler, error) {
	if partitions <= 0 || bits.OnesCount(uint(partitions)) != 1 {
		return nil, fmt.Errorf("partitions must be a power of 2 (e.g., 2, 4, 8, 16), got: %d", partitions)
	}

	outChannels := make([]chan types.MappedRecord, partitions)
	for i := 0; i < partitions; i++ {
		outChannels[i] = make(chan types.MappedRecord, channelBufferSize)
	}

	return &Shuffler{
		partitions: uint64(partitions),
		mask:       uint64(partitions - 1), // e.g., for 16 partitions, mask is 15 (00001111 in binary)
		outbound:   outChannels,
	}, nil
}

func (s *Shuffler) Start(ctx context.Context, in <-chan types.MappedRecord) {
	go func() {
		defer s.closeAll()

		for {
			select {
			case <-ctx.Done():
				return
			case record, ok := <-in:
				if !ok {
					return
				}

				// The Bitwise Trick: identical to (DimensionHash % partitions), but vastly faster
				routeIdx := record.DimensionHash & s.mask

				select {
				case <-ctx.Done():
					return
				case s.outbound[routeIdx] <- record: // Push directly to the dedicated Reducer channel
				}
			}
		}
	}()
}

func (s *Shuffler) GetPartitions() []<-chan types.MappedRecord {
	readOnly := make([]<-chan types.MappedRecord, len(s.outbound))
	for i, ch := range s.outbound {
		readOnly[i] = ch
	}
	return readOnly
}

func (s *Shuffler) closeAll() {
	for _, ch := range s.outbound {
		close(ch)
	}
}

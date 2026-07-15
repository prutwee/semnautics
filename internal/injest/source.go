package ingest

import (
	"context"

	"github.com/prutwee/semnautics/pkg/types"
)

type Source interface {
	Start(ctx context.Context, out chan<- types.Event) error

	Stop() error
}

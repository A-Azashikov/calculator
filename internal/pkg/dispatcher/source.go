package dispatcher

import (
	"context"
	"sync"

	"io.github.a_azashikov/calculator/internal/pkg/domain"
)

type SourceHandler func(ctx context.Context, params []SourceParam) <-chan domain.CalculationTask

type SourceDispatcher struct {
	batchSize int

	registry map[string]SourceHandler
}

type SourceParam struct {
	ID        string
	Timestamp int64
}

func NewSourceDispatcher(batchSize int) *SourceDispatcher {
	s := &SourceDispatcher{
		batchSize: batchSize,
		registry:  make(map[string]SourceHandler),
	}

	return s
}

func (s *SourceDispatcher) RegisterSource(sourceId string, handler SourceHandler) {
	s.registry[sourceId] = handler
}

type batchParam struct {
	sourceId string
	params   []SourceParam
}

func (s *SourceDispatcher) Start(ctx context.Context, in <-chan domain.CalculationTask) <-chan domain.CalculationTask {
	out := make(chan domain.CalculationTask, len(s.registry)*s.batchSize)
	batchesChan := make(chan batchParam, len(s.registry)*s.batchSize)
	workersCount := len(s.registry) * 8

	go func() {
		var wg sync.WaitGroup
		defer close(out)

		for range workersCount {
			wg.Go(func() {
				for batch := range batchesChan {
					s.loadSource(ctx, batch.sourceId, batch.params, out)
				}
			})
		}

		sourceBatches := make(map[string][]SourceParam)
		for task := range in {
			select {
			case <-ctx.Done():
				return
			default:
			}

			switch _, ok := s.registry[task.Expression]; ok {
			case true:
				batches := sourceBatches[task.Expression]
				batches = append(batches, SourceParam{
					ID:        task.Key.ID,
					Timestamp: task.Key.Timestamp,
				})
				if len(batches) == s.batchSize {
					batchesChan <- batchParam{sourceId: task.Expression, params: batches}
					batches = nil
				}
				sourceBatches[task.Expression] = batches
			default:
				select {
				case <-ctx.Done():
					return
				case out <- task:
				}
			}
		}

		for k, v := range sourceBatches {
			if len(v) < 1 {
				continue
			}
			batchesChan <- batchParam{sourceId: k, params: v}
		}

		close(batchesChan)

		wg.Wait()
	}()

	return out
}

func (s *SourceDispatcher) loadSource(ctx context.Context, sourceId string, params []SourceParam, out chan domain.CalculationTask) {
	tasks := s.registry[sourceId](ctx, params)

	for task := range tasks {
		select {
		case <-ctx.Done():
			return
		case out <- task:
		}
	}
}

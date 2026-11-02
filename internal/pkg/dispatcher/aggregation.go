package dispatcher

import (
	"context"
	"fmt"
	"sync"
	"time"

	"io.github.a_azashikov/calculator/internal/pkg/domain"
)

type AggregationDispatcher struct {
	windowType domain.WindowType

	dateStart string
	dateEnd   string

	buckets map[domain.AggregationTaskKey]domain.AggregationBucket
	workers []func(task domain.AggregationTask) domain.AggregationResult
}

func NewAggregationDispatcher(windowType domain.WindowType, dateStart string, dateEnd string) *AggregationDispatcher {
	s := &AggregationDispatcher{
		windowType: windowType,

		dateStart: dateStart,
		dateEnd:   dateEnd,

		buckets: make(map[domain.AggregationTaskKey]domain.AggregationBucket),

		workers: make([]func(task domain.AggregationTask) domain.AggregationResult, 0),
	}

	return s
}

func (a *AggregationDispatcher) AddWorker(worker func(task domain.AggregationTask) domain.AggregationResult) {
	a.workers = append(a.workers, worker)
}

type window struct {
	left  int64
	right int64
	size  int
}

type windowKey struct {
	left       int64
	windowType domain.WindowType
}

type factorWindowKey struct {
	id         string
	left       int64
	windowType domain.WindowType
}

func (a *AggregationDispatcher) Start(ctx context.Context, in <-chan domain.CalculationResult) <-chan domain.AggregationResult {
	workersCount := len(a.workers)

	out := make(chan domain.AggregationResult, workersCount*2)
	tasksChan := make(chan domain.AggregationTask, workersCount*2)

	var workerWg sync.WaitGroup
	for _, worker := range a.workers {
		workerWg.Go(func() {
			for task := range tasksChan {
				out <- worker(task)
			}
		})
	}

	layout := "2006-01-02"

	windowType := a.windowType
	windows, _ := splitPeriod(a.dateStart, a.dateEnd, windowType)
	minWindowLeft := windows[0].left
	windowChan := make(map[windowKey]chan domain.CalculationResult)

	var windowsWg sync.WaitGroup
	for _, window := range windows {
		resChan := make(chan domain.CalculationResult, len(windows)*2)
		windowChan[windowKey{left: window.left, windowType: windowType}] = resChan

		windowsWg.Go(func() {
			windowValuesMap := make(map[factorWindowKey][]domain.CalculationResult)
			for result := range resChan {
				select {
				case <-ctx.Done():
					return
				default:
				}
				factorKey := factorWindowKey{id: result.Key.ID, left: window.left, windowType: windowType}
				windowValues := windowValuesMap[factorKey]
				windowValues = append(windowValues, result)
				if len(windowValues) == window.size {
					tasksChan <- domain.AggregationTask{
						Key: domain.AggregationTaskKey{
							ID:          result.Key.ID,
							WindowStart: time.Unix(window.left, 0).Format(layout),
							WindowType:  windowType,
						},
						Values: windowValues,
					}
					windowValues = nil
				}
				windowValuesMap[factorKey] = windowValues
			}
		})
	}

	go func() {
		for _, c := range windowChan {
			defer close(c)
		}
		for calc := range in {
			select {
			case <-ctx.Done():
				return
			default:
			}
			key := windowKey{
				left:       max(alignToWindowStart(calc.Key.Timestamp, windowType), minWindowLeft),
				windowType: windowType,
			}
			windowChan[key] <- calc
		}
	}()

	go func() {
		defer close(out)
		workerWg.Wait()
	}()

	go func() {
		defer close(tasksChan)
		windowsWg.Wait()
	}()

	return out
}

func splitPeriod(startDate, endDate string, windowType domain.WindowType) ([]window, error) {
	const layout = "2006-01-02"

	start, err := time.Parse(layout, startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid start date: %w", err)
	}
	end, err := time.Parse(layout, endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid end date: %w", err)
	}

	if start.After(end) {
		return nil, fmt.Errorf("start date cannot be after end date")
	}

	globalEnd := end.AddDate(0, 0, 1)

	var windows []window
	currentStart := start

	for currentStart.Before(globalEnd) {
		var currentEnd time.Time

		switch windowType {
		case domain.WindowDay:
			currentEnd = currentStart.AddDate(0, 0, 1)
		case domain.WindowMonth:
			currentEnd = currentStart.AddDate(0, 1, 0)
			currentEnd = time.Date(currentEnd.Year(), currentEnd.Month(), 1, 0, 0, 0, 0, currentEnd.Location())
		case domain.WindowQuarter:
			currentEnd = currentStart.AddDate(0, 3, 0)
			quarter := int(currentEnd.Month() / 3)
			currentEnd = time.Date(currentEnd.Year(), time.Month(quarter*3+1), 1, 0, 0, 0, 0, currentEnd.Location())
		case domain.WindowYear:
			currentEnd = currentStart.AddDate(1, 0, 0)
			currentEnd = time.Date(currentEnd.Year(), time.January, 1, 0, 0, 0, 0, currentEnd.Location())
		default:
			return nil, fmt.Errorf("unknown window type")
		}

		if currentEnd.After(globalEnd) {
			currentEnd = globalEnd
		}

		days := int(currentEnd.Sub(currentStart).Hours() / 24)

		windows = append(windows, window{
			left:  currentStart.Unix(),
			right: currentEnd.Unix(),
			size:  days,
		})

		currentStart = currentEnd
	}

	return windows, nil
}

func alignToWindowStart(timestamp int64, winType domain.WindowType) int64 {
	loc := time.UTC

	t := time.Unix(timestamp, 0)

	var aligned time.Time

	switch winType {
	case domain.WindowDay:
		aligned = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)

	case domain.WindowMonth:
		aligned = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc)

	case domain.WindowQuarter:
		month := t.Month()
		quarter := int(month / 3)
		aligned = time.Date(t.Year(), time.Month(quarter*3+1), 1, 0, 0, 0, 0, loc)

	case domain.WindowYear:
		aligned = time.Date(t.Year(), time.January, 1, 0, 0, 0, 0, loc)

	default:
		aligned = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
	}

	return aligned.Unix()
}

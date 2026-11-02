package dispatcher

import (
	"container/list"
	"context"
	"sync"

	"io.github.a_azashikov/calculator/internal/pkg/domain"
)

type CalculationDispatcher struct {
	definitions  map[domain.CalculationTaskKey]domain.CalculationTask
	dependencies map[domain.CalculationTaskKey][]domain.CalculationTaskKey

	completed   map[domain.CalculationTaskKey]float64
	pendingDeps map[domain.CalculationTaskKey]int

	readyTaskQueue *list.List

	workers []func(domain.CalculationTask) domain.CalculationResult
}

func (c *CalculationDispatcher) AddWorker(worker func(task domain.CalculationTask) domain.CalculationResult) {
	c.workers = append(c.workers, worker)
}

func NewCalculationDispatcher() *CalculationDispatcher {
	s := &CalculationDispatcher{
		definitions:  make(map[domain.CalculationTaskKey]domain.CalculationTask),
		dependencies: make(map[domain.CalculationTaskKey][]domain.CalculationTaskKey),
		completed:    make(map[domain.CalculationTaskKey]float64),
		pendingDeps:  make(map[domain.CalculationTaskKey]int),

		readyTaskQueue: list.New(),
		workers:        make([]func(domain.CalculationTask) domain.CalculationResult, 0),
	}

	return s
}

func (c *CalculationDispatcher) Start(ctx context.Context, in <-chan domain.CalculationTask) <-chan domain.CalculationResult {
	workersCount := len(c.workers)

	out := make(chan domain.CalculationResult, workersCount*2)

	tasksChan := make(chan domain.CalculationTask, workersCount*2)
	resultsChan := make(chan domain.CalculationResult, workersCount*2)

	var wg sync.WaitGroup
	for _, worker := range c.workers {
		wg.Go(func() {
			for task := range tasksChan {
				result := worker(task)
				resultsChan <- result
			}
		})
	}

	go func() {
		defer close(resultsChan)
		wg.Wait()
	}()

	go func() {
		defer close(out)
		defer close(tasksChan)

		receivedTasks, completedTasks := 0, 0
		sourceIsEmpty := false

		for {
			var nextTaskElement *list.Element
			var nextTask domain.CalculationTask
			var targetChan chan domain.CalculationTask
			var sourceChan <-chan domain.CalculationTask

			if !sourceIsEmpty {
				sourceChan = in
			}

			if c.readyTaskQueue.Len() > 0 {
				nextTaskElement = c.readyTaskQueue.Front()
				nextTask = nextTaskElement.Value.(domain.CalculationTask)
				targetChan = tasksChan
			}

			select {
			case d, ok := <-sourceChan:
				if !ok {
					sourceIsEmpty = true
					continue
				}
				receivedTasks++
				c.handleTask(d)
			case targetChan <- nextTask:
				c.readyTaskQueue.Remove(nextTaskElement)
			case res := <-resultsChan:
				completedTasks++
				c.handleResult(res)
				out <- res
			case <-ctx.Done():
				return
			}

			if sourceIsEmpty && receivedTasks == completedTasks {
				return
			}
		}
	}()

	return out
}

func (c *CalculationDispatcher) handleResult(res domain.CalculationResult) {
	c.completed[res.Key] = res.Value

	children := c.dependencies[res.Key]
	for _, childKey := range children {
		c.pendingDeps[childKey]--
		if c.pendingDeps[childKey] == 0 {
			c.readyTaskQueue.PushBack(c.generateChildTask(childKey))
		}
	}
}

func (c *CalculationDispatcher) generateChildTask(childKey domain.CalculationTaskKey) domain.CalculationTask {
	task := c.definitions[childKey]
	values := make([]domain.ExpressionVariableValue, len(task.Variables))
	for i, variable := range task.Variables {
		values[i] = domain.ExpressionVariableValue{Name: variable.Name, Value: c.completed[domain.CalculationTaskKey{ID: variable.ID, Timestamp: task.Key.Timestamp}]}
	}
	newTask := domain.CalculationTask{
		Key:             task.Key,
		Expression:      task.Expression,
		ExpressionAst:   task.ExpressionAst,
		Variables:       task.Variables,
		VariableResults: values,
	}
	return newTask
}

func (c *CalculationDispatcher) handleTask(task domain.CalculationTask) {
	c.definitions[task.Key] = task
	c.pendingDeps[task.Key] = len(task.Variables)

	for _, variable := range task.Variables {
		parentKey := domain.CalculationTaskKey{ID: variable.ID, Timestamp: task.Key.Timestamp}
		c.dependencies[parentKey] = append(c.dependencies[parentKey], task.Key)
		_, completed := c.completed[parentKey]
		if completed {
			c.pendingDeps[task.Key]--
		}
	}
	if c.pendingDeps[task.Key] == 0 {
		c.readyTaskQueue.PushBack(c.generateChildTask(task.Key))
	}
}

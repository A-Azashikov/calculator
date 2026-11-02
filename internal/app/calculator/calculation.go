package calculator

import (
	"context"
	"fmt"
	"time"

	"io.github.a_azashikov/calculator/internal/pkg/dispatcher"
	"io.github.a_azashikov/calculator/internal/pkg/domain"
	"io.github.a_azashikov/calculator/internal/pkg/utils"
)

func RunCalculation() {

	const calculationWorkerNum = 12
	const aggregationWorkerNum = 4
	const startDate = "2024-01-01"
	const endDate = "2024-12-31"

	s := dispatcher.NewSourceDispatcher(500)
	s.RegisterSource("source_1", loadFromSourceWorker)
	s.RegisterSource("source_2", loadFromSourceWorker)
	s.RegisterSource("source_3", loadFromSourceWorker)
	s.RegisterSource("source_4", loadFromSourceWorker)

	c := dispatcher.NewCalculationDispatcher()
	for range calculationWorkerNum {
		c.AddWorker(calculationWorker)
	}

	a := dispatcher.NewAggregationDispatcher(domain.WindowYear, startDate, endDate)
	for range aggregationWorkerNum {
		a.AddWorker(aggregationWorker)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	aggChan := a.Start(
		ctx,
		c.Start(
			ctx,
			s.Start(
				ctx,
				// Симуляция получения списка задач для расчетов связанных показателей (1000 показателей, максимальный уровень вложенности 10)
				utils.GenerateTasks(startDate, endDate, 1000, 10),
			),
		),
	)

	results := make(map[domain.AggregationTaskKey]float64)
	for result := range aggChan {
		results[result.Key] = result.Value
	}
	fmt.Printf("Агрегация завершила работу. Получила %d элементов.\n", len(results))
	fmt.Println("Расчет завершился")
}

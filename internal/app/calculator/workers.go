package calculator

import (
	"context"
	"math/rand/v2"
	"time"

	"io.github.a_azashikov/calculator/internal/pkg/dispatcher"
	"io.github.a_azashikov/calculator/internal/pkg/domain"
	"io.github.a_azashikov/calculator/internal/pkg/utils"
)

func calculationWorker(task domain.CalculationTask) domain.CalculationResult {
	variableValues := make(map[string]float64)

	for _, res := range task.VariableResults {
		variableValues[res.Name] = res.Value
	}

	value := utils.EvaluateNode(*task.ExpressionAst, variableValues)
	return domain.CalculationResult{Key: task.Key, Value: value}
}

func aggregationWorker(task domain.AggregationTask) domain.AggregationResult {
	sum := .0
	for _, v := range task.Values {
		sum += v.Value
	}
	return domain.AggregationResult{Key: task.Key, Value: sum}
}

func loadFromSourceWorker(ctx context.Context, params []dispatcher.SourceParam) <-chan domain.CalculationTask {
	out := make(chan domain.CalculationTask, len(params))

	go func() {
		expression := "A"
		expressionAst := utils.ParseExpression(expression)
		defer close(out)
		time.Sleep(time.Millisecond * 500)
		for _, p := range params {
			select {
			case <-ctx.Done():
				return
			case out <- domain.CalculationTask{
				Key: domain.CalculationTaskKey{
					ID:        p.ID,
					Timestamp: p.Timestamp,
				},
				Expression:      expression,
				ExpressionAst:   &expressionAst,
				VariableResults: []domain.ExpressionVariableValue{{Name: "A", Value: rand.Float64()}},
			}:
			}
		}
	}()

	return out
}

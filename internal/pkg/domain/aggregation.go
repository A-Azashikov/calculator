package domain

type WindowType int

const (
	WindowDay WindowType = iota
	WindowMonth
	WindowQuarter
	WindowYear
)

type AggregationBucket struct {
	Size   int
	Values []CalculationResult
}

type AggregationTask struct {
	Key    AggregationTaskKey
	Values []CalculationResult
}

type AggregationTaskKey struct {
	ID          string
	WindowStart string
	WindowType  WindowType
}

type AggregationResult struct {
	Key   AggregationTaskKey
	Value float64
}

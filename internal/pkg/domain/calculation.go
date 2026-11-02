package domain

import "go/ast"

type CalculationTaskKey struct {
	ID        string
	Timestamp int64
}

type CalculationResult struct {
	Key   CalculationTaskKey
	Value float64
}

type ExpressionVariable struct {
	Name string
	ID   string
}

type ExpressionVariableValue struct {
	Name  string
	Value float64
}

type CalculationTask struct {
	Key             CalculationTaskKey
	Expression      string
	ExpressionAst   *ast.Expr
	Variables       []ExpressionVariable
	VariableResults []ExpressionVariableValue
}

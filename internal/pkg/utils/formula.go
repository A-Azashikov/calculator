package utils

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
)

func ParseExpression(expression string) ast.Expr {
	expr, err := parser.ParseExpr(expression)
	if err != nil {
		panic(fmt.Sprintf("Ошибка парсинга формулы: %v", err))
	}
	return expr
}

func EvaluateNode(node ast.Node, variableValues map[string]float64) float64 {
	switch n := node.(type) {
	case *ast.BinaryExpr:
		switch n.Op {
		case token.ADD:
			return EvaluateNode(n.X, variableValues) + EvaluateNode(n.Y, variableValues)
		case token.SUB:
			return EvaluateNode(n.X, variableValues) - EvaluateNode(n.Y, variableValues)
		case token.MUL:
			return EvaluateNode(n.X, variableValues) * EvaluateNode(n.Y, variableValues)
		case token.QUO:
			return EvaluateNode(n.X, variableValues) / EvaluateNode(n.Y, variableValues)
		}
	case *ast.Ident:
		return variableValues[n.Name]
	case *ast.BasicLit:
		v, _ := strconv.ParseFloat(n.Value, 64)
		return v
	case *ast.ParenExpr:
		return EvaluateNode(n.X, variableValues)
	default:
		fmt.Println(n)
	}
	return .0
}

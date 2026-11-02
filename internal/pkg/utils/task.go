package utils

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"io.github.a_azashikov/calculator/internal/pkg/domain"
)

type result struct {
	factors     []string
	expressions []string
	factorDeps  map[string][]string
}

type generator struct {
	sources      []string
	maxDepth     int
	factorsCount int

	result result
}

func newGenerator(factorsCount int, maxDepth int) *generator {
	return &generator{
		sources:      []string{"source_1", "source_2", "source_3", "source_4"},
		maxDepth:     maxDepth,
		factorsCount: factorsCount,
		result: result{
			factorDeps: make(map[string][]string),
		},
	}
}

func toExpression(deps []string) string {
	ops := []string{" + ", " - ", " * ", " / "}

	var sb strings.Builder
	sb.WriteString(deps[0])

	for _, dep := range deps[1:] {
		randomOp := ops[rand.Intn(len(ops))]
		sb.WriteString(randomOp)
		sb.WriteString(dep)
	}

	return fmt.Sprintf("(%s)", sb.String())
}

func (g *generator) generate() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	g.result.factors = make([]string, g.factorsCount)
	for i := 0; i < g.factorsCount; i++ {
		g.result.factors[i] = fmt.Sprintf("Factor_%d", i)
	}

	g.result.expressions = make([]string, g.factorsCount)

	levels := make(map[int][]string)
	factorLevels := make(map[string]int)

	for i, factor := range g.result.factors {
		var depth int
		if i == 0 || g.maxDepth == 0 {
			depth = 0
		} else {
			depth = r.Intn(g.maxDepth + 1)
		}

		levels[depth] = append(levels[depth], factor)
		factorLevels[factor] = depth

		if depth == 0 {
			g.result.expressions[i] = g.sources[r.Intn(len(g.sources))]
			continue
		}

		var availableDeps []string
		for d := 0; d < depth; d++ {
			availableDeps = append(availableDeps, levels[d]...)
		}

		if len(availableDeps) == 0 {
			g.result.expressions[i] = g.sources[r.Intn(len(g.sources))]
			continue
		}

		maxDepsNum := 3
		if len(availableDeps) < maxDepsNum {
			maxDepsNum = len(availableDeps)
		}
		depsCount := r.Intn(maxDepsNum) + 1

		r.Shuffle(len(availableDeps), func(i, j int) {
			availableDeps[i], availableDeps[j] = availableDeps[j], availableDeps[i]
		})

		chosenDeps := availableDeps[:depsCount]
		g.result.factorDeps[factor] = chosenDeps

		g.result.expressions[i] = toExpression(chosenDeps)
	}
}

func GenerateTasks(startDate string, endDate string, factorCount int, factorDepth int) chan domain.CalculationTask {
	tasksChan := make(chan domain.CalculationTask, factorCount)

	gen := newGenerator(factorCount, factorDepth)
	gen.generate()
	factors := gen.result.factors
	expressions := gen.result.expressions
	factorDeps := gen.result.factorDeps

	dates, err := GenerateDateRange(startDate, endDate, "2006-01-02")
	if err != nil {
		defer close(tasksChan)
		fmt.Println("Ошибка:", err)
		return tasksChan
	}

	go func() {
		defer close(tasksChan)
		totalGenerated := 0
		for i, factor := range factors {
			exprAst := ParseExpression(expressions[i])

			for _, date := range dates {
				expression := expressions[i]
				key := domain.CalculationTaskKey{ID: factor, Timestamp: date}
				variables := make([]domain.ExpressionVariable, 0)

				for _, dep := range factorDeps[factor] {
					variables = append(variables, domain.ExpressionVariable{ID: dep, Name: dep})
				}
				tasksChan <- domain.CalculationTask{Key: key, Expression: expression, ExpressionAst: &exprAst, Variables: variables}
				totalGenerated++
			}
		}
		fmt.Println("Total generated tasks count ", totalGenerated)
	}()
	return tasksChan
}

package utils

import (
	"fmt"
	"time"
)

func ParseDate(date string) (int64, error) {

	start, err := time.Parse("2006-01-02", date)
	if err != nil {
		return 0, fmt.Errorf("ошибка парсинга начальной даты: %w", err)
	}

	return start.Unix(), nil
}

func GenerateDateRange(startStr, endStr, layout string) ([]int64, error) {
	var dates []int64

	start, err := time.Parse(layout, startStr)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга начальной даты: %w", err)
	}

	end, err := time.Parse(layout, endStr)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга конечной даты: %w", err)
	}

	if start.After(end) {
		return dates, nil
	}

	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		dates = append(dates, d.Unix())
	}

	return dates, nil
}

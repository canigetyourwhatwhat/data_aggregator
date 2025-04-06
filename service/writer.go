package service

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strconv"
)

func WriteCSV(filePath string, data map[int]map[string]float64) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	w := csv.NewWriter(file)
	defer w.Flush()

	if err = w.Write([]string{"Household ID", "Quarter", "Total Consumption  (KWh)"}); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Sort household IDs
	householdIDs := make([]int, len(data))
	for householdID := range data {
		householdIDs = append(householdIDs, householdID)
	}
	sort.Ints(householdIDs)

	// Write each household's data
	for _, household := range householdIDs {
		quarters := data[household]

		// Sort quarters
		var qList []string
		for q := range quarters {
			qList = append(qList, q)
		}
		sort.Strings(qList)

		// Write each quarter's data
		for _, q := range qList {
			if err = w.Write([]string{
				strconv.Itoa(household),
				q,
				strconv.FormatFloat(quarters[q], 'f', 2, 64),
			}); err != nil {
				return fmt.Errorf("failed to write data: %w", err)
			}
		}
	}
	return nil
}

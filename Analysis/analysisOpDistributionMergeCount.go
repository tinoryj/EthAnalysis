package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	// Sample file list (replace with actual file paths)
	files := []string{"countKVDis-0-block-20500000-20600000/countKVDis.txt", "countKVDis-1-block-20600000-20700000/countKVDis.txt", "countKVDis-2-block-20700000-20800000/countKVDis.txt", "countKVDis-3-block-20700000-20800000/countKVDis.txt", "countKVDis-4-block-20800000-20900000/countKVDis.txt", "countKVDis-5-block-20900000-21000000/countKVDis.txt", "countKVDis-6-block-21000000-21100000/countKVDis.txt", "countKVDis-7-block-21100000-21200000/countKVDis.txt", "countKVDis-8-block-21200000-21300000/countKVDis.txt", "countKVDis-9-block-21300000-21400000/countKVDis.txt", "countKVDis-10-block-21400000-21500000/countKVDis.txt"}

	// A map to store the aggregated counts: category -> opType -> count
	aggregatedData := make(map[string]map[string]uint64)

	// Process each file
	for _, fileName := range files {
		err := processFile(fileName, aggregatedData)
		if err != nil {
			log.Printf("Error processing file %s: %v\n", fileName, err)
		}
	}

	// Output the aggregated data
	printAggregatedData(aggregatedData)
}

// processFile processes a single file and aggregates data
func processFile(fileName string, aggregatedData map[string]map[string]uint64) error {
	file, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %v", fileName, err)
	}
	defer file.Close()
	totalOpCount := 0
	scanner := bufio.NewScanner(file)
	var currentCategory string
	for scanner.Scan() {
		line := scanner.Text()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// If the line starts with "Category", it's a new category
		if strings.HasPrefix(line, "Category: ") {
			currentCategory = strings.TrimPrefix(line, "Category: ")
			// Initialize the category in the map if not already present
			if _, exists := aggregatedData[currentCategory]; !exists {
				aggregatedData[currentCategory] = make(map[string]uint64)
			}
		} else if strings.HasPrefix(line, "  OPType: ") {
			// Extract the OPType and Count
			parts := strings.Fields(line)
			if len(parts) != 4 {
				continue // skip invalid lines
			}
			opType := strings.Trim(parts[1], ",")
			count := parseCount(parts[3])

			// Aggregate the count for the current category and OPType
			aggregatedData[currentCategory][opType] += count
			totalOpCount += int(count)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %v", fileName, err)
	}
	fmt.Printf("%d\n", totalOpCount)
	return nil
}

// parseCount parses the count from the string and returns it as uint64
func parseCount(countStr string) uint64 {
	var count uint64
	fmt.Sscanf(countStr, "%d", &count)
	return count
}

// printAggregatedData prints the aggregated data in the required format
func printAggregatedData(aggregatedData map[string]map[string]uint64) {
	fmt.Println("Count of KV operations:")
	for category, opTypes := range aggregatedData {
		fmt.Printf("Category: %s\n", category)
		for opType, count := range opTypes {
			fmt.Printf("  OPType: %s, Count: %d\n", opType, count)
		}
	}
}

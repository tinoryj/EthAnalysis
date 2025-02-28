package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
)

// PearsonResult stores the pairwise Pearson coefficient results
type PearsonResult struct {
	Key1  string
	Key2  string
	Coeff float64
}

func main() {
	// Input files
	log1File := "log-1.txt"
	log2File := "log-2.txt"

	// Regex for identifying partitions
	startRegex := regexp.MustCompile(`Processing block \(start\), ID: (\d+)`)
	endRegex := regexp.MustCompile(`Processing block \(end\), ID: (\d+)`)

	// Read the 100 keys from log-2
	keys, err := readLog2Keys(log2File)
	if err != nil {
		fmt.Printf("Error reading log-2: %v\n", err)
		return
	}

	// the block number per batch:
	// 5 block/min; 300 block/hour; 7200 block/day
	blockNumPerBatch := 5

	// Process log-1 and compute Pearson coefficients
	accumulatedResults := processLog1AndCompute(log1File, startRegex, endRegex, keys, blockNumPerBatch)

	// Output results
	fmt.Println("Final Accumulated Pearson Coefficients:")
	for i, result := range accumulatedResults {
		fmt.Printf("%s - %s: %.6f\n", result.Key1, result.Key2, result.Coeff)
		if (i+1)%10 == 0 {
			fmt.Println()
		}
	}
}

// readLog2Keys parses the 100 keys from log-2
func readLog2Keys(log2File string) ([]string, error) {
	file, err := os.Open(log2File)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var keys []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			keys = append(keys, parts[1])
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

// processLog1AndCompute processes log-1 line by line to compute Pearson coefficients
func processLog1AndCompute(log1File string, startRegex, endRegex *regexp.Regexp, keys []string, blockNumPerBatch int) []PearsonResult {
	file, err := os.Open(log1File)
	if err != nil {
		fmt.Printf("Error opening log-1: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	// Create accumulators and other necessary variables
	numKeys := len(keys)
	accumulatedResults := make([]float64, numKeys*numKeys) // Accumulator for Pearson coefficients
	keyIndexMap := make(map[string]int)
	for i, key := range keys {
		keyIndexMap[key] = i
	}

	// Variables for per-partition processing
	var inPartition bool
	bitSequences := initializeBitSequences(numKeys)
	lineIndex := 0    // Tracks the "position" (bit index) in the current partition
	blockCounter := 0 // Tracks the number of blocks processed in the current batch

	for {
		line, err := reader.ReadString('\n')
		if err != nil { // End of file
			break
		}
		line = strings.TrimSpace(line)

		// Check for start of a block
		if startRegex.MatchString(line) {
			inPartition = true
			bitSequences = initializeBitSequences(numKeys) // Reset bit sequences for new block
			lineIndex = 0                                  // Reset line index
			continue
		}

		// Check for end of a block
		if endRegex.MatchString(line) {
			if inPartition {
				blockCounter++
				if blockCounter == blockNumPerBatch {
					// Compute Pearson coefficients for the current batch
					pearsonResults := computePearsonCoefficients(bitSequences)

					// Accumulate results
					for i := range pearsonResults {
						accumulatedResults[i] += pearsonResults[i]
					}

					// Reset for the next batch
					blockCounter = 0
					bitSequences = initializeBitSequences(numKeys)
					lineIndex = 0
				}
			}

			// Clear partition variables
			inPartition = false
			continue
		}

		// Process lines within a partition
		if inPartition {
			processLine(line, keys, keyIndexMap, bitSequences, lineIndex)
			lineIndex++
		}
	}

	// Convert accumulated results to PearsonResult format
	var results []PearsonResult
	for i := 0; i < numKeys; i++ {
		for j := 0; j < numKeys; j++ {
			results = append(results, PearsonResult{
				Key1:  keys[i],
				Key2:  keys[j],
				Coeff: accumulatedResults[i*numKeys+j],
			})
		}
	}
	return results
}

// initializeBitSequences initializes a 2D slice for bit sequences
func initializeBitSequences(numKeys int) [][]int {
	bitSequences := make([][]int, numKeys)
	for i := range bitSequences {
		bitSequences[i] = []int{}
	}
	return bitSequences
}

// processLine updates the bit sequences for the current line
func processLine(line string, keys []string, keyIndexMap map[string]int, bitSequences [][]int, lineIndex int) {
	// Ensure each sequence in bitSequences has enough bits for the current lineIndex
	for i := range bitSequences {
		if len(bitSequences[i]) <= lineIndex {
			bitSequences[i] = append(bitSequences[i], 0) // Extend with zero
		}
	}

	// Parse the line to find keys
	parts := strings.Split(line, ";")
	if len(parts) < 2 {
		return
	}
	keyPart := strings.TrimPrefix(parts[0], "key: ")
	keysInLine := strings.Split(keyPart, ";")

	// Set bit for each key that appears in the current line
	for _, key := range keysInLine {
		if idx, found := keyIndexMap[key]; found {
			bitSequences[idx][lineIndex] = 1
		}
	}
}

// computePearsonCoefficients calculates the Pearson coefficients for all key pairs
func computePearsonCoefficients(bitSequences [][]int) []float64 {
	numKeys := len(bitSequences)
	pearsonResults := make([]float64, numKeys*numKeys)

	for i := 0; i < numKeys; i++ {
		for j := 0; j < numKeys; j++ {
			if i == j {
				pearsonResults[i*numKeys+j] = 1.0 // Self-correlation
				continue
			}
			pearsonResults[i*numKeys+j] = calculatePearson(bitSequences[i], bitSequences[j])
		}
	}

	return pearsonResults
}

// calculatePearson computes the Pearson correlation coefficient for two bit sequences
func calculatePearson(x, y []int) float64 {
	if len(x) != len(y) {
		return 0.0
	}

	n := len(x)
	var sumX, sumY, sumXY, sumX2, sumY2 float64

	for i := 0; i < n; i++ {
		sumX += float64(x[i])
		sumY += float64(y[i])
		sumXY += float64(x[i] * y[i])
		sumX2 += float64(x[i] * x[i])
		sumY2 += float64(y[i] * y[i])
	}

	numerator := float64(n)*sumXY - sumX*sumY
	denominator := math.Sqrt((float64(n)*sumX2 - sumX*sumX) * (float64(n)*sumY2 - sumY*sumY))

	if denominator == 0 {
		return 0.0
	}
	return numerator / denominator
}

package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
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
	log1File := "/home/tinoryj/geth-trace-2025-02-11-19-18-38"
	// log2File := "/home/jzhao/categories_top100.log"
	log2File := "/home/jzhao/overall_get_top100.log"

	// Read the 100 keys from log-2
	keys, err := readLog2Keys(log2File)
	if err != nil {
		fmt.Printf("Error reading log-2: %v\n", err)
		return
	}

	// The block number per batch:
	// 5 block/min; 300 block/hour; 7200 block/day
	blockNumPerBatch := 5

	// Process log-1 and compute Pearson coefficients
	accumulatedResults := processLog1AndCompute(log1File, keys, blockNumPerBatch)

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
func processLog1AndCompute(log1File string, keys []string, blockNumPerBatch int) []PearsonResult {
	file, err := os.Open(log1File)
	if err != nil {
		fmt.Printf("Error opening log-1: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Regex to match block start and end lines
	startRegex := regexp.MustCompile(`Processing block \(start\), ID: (\d+)`)
	endRegex := regexp.MustCompile(`Processing block \(end\), ID: (\d+)`)
	opGetRegex := regexp.MustCompile(`OPType: Get, key: ([0-9a-fA-F]+), size: (\d+)`)

	// Variables to track the current block
	var currentBlockID string

	var outputFile *os.File
	if outputFile != nil {
		outputFile.Close()
	}

	reader := bufio.NewReader(file)

	var lineCount uint64
	lineCount = 0

	var foundStartID bool
	foundStartID = false

	// for test
	// batchStartIDs := []int{20499866, 20500000}
	// batchEndIDs := []int{20499999, 20500100}
	batchStartIDs := []int{20500000}
	batchEndIDs := []int{20759721}
	var batchIndex int
	batchIndex = 0

	// Create accumulators and other necessary variables
	numKeys := len(keys)
	accumulatedResults := make([]float64, numKeys*numKeys) // Accumulator for Pearson coefficients
	keyIndexMap := make(map[string]int)
	for i, key := range keys {
		keyIndexMap[key] = i
	}

	// Variables for per-partition processing
	bitSequences := initializeBitSequences(numKeys)
	lineIndex := 0    // Tracks the "position" (bit index) in the current partition
	blockCounter := 0 // Tracks the number of blocks processed in the current batch

	for {

		// Read the file line by line
		lineCount++

		if lineCount%10000 == 0 {
			fmt.Printf("\rProcessed %d lines", lineCount)
		}

		line, err := reader.ReadString('\n')
		if err != nil { // End of file
			break
		}
		line = strings.TrimSpace(line)

		// Check if the line is the start of a block
		if matches := startRegex.FindStringSubmatch(line); matches != nil {

			fmt.Printf("in start\n")

			// Extract the block ID
			currentBlockID = matches[1]

			// Convert block ID to integer
			blockIDInt, err := strconv.Atoi(currentBlockID)
			if err != nil {
				blockIDInt = 0
				fmt.Println("Error converting current block ID to integer: %d", err, blockIDInt)
			}

			tmpBatchIndex := findIndex(blockIDInt, batchStartIDs)

			if tmpBatchIndex != -1 {
				batchIndex = tmpBatchIndex
				foundStartID = true
			}

			// bitSequences = initializeBitSequences(numKeys) // Reset bit sequences for new block
			// lineIndex = 0                                  // Reset line index
		}

		// Skip all lines until we have found the line with startID
		if !foundStartID {
			continue
		}

		// If inside a block, check for "OPType: Get" lines
		if opGetRegex.MatchString(line) {
			fmt.Printf("in get\n")

			// prepare the bit sequence (vector)
			processLine(line, keys, keyIndexMap, bitSequences, lineIndex)
			lineIndex++
		}

		// Check if the line is the end of the block
		if matches := endRegex.FindStringSubmatch(line); matches != nil {
			fmt.Printf("in end\n")

			// Verify the block ID matches
			endBlockID := matches[1]
			if endBlockID != currentBlockID {
				fmt.Printf("block ID mismatch: start ID %s, end ID %s", currentBlockID, endBlockID)
			}

			blockCounter ++
			if blockCounter == blockNumPerBatch {
				// Compute Pearson coefficients for the current batch
				pearsonResults := computePearsonCoefficients(bitSequences, lineIndex)

				// Accumulate results
				for i := range pearsonResults {
					accumulatedResults[i] += pearsonResults[i]
				}

				// Reset for the next batch
				blockCounter = 0
				bitSequences = initializeBitSequences(numKeys)
				lineIndex = 0
			}

			endIDInt, err := strconv.Atoi(endBlockID)
			if err != nil {
				fmt.Println("Error converting end block ID to integer:", err)
			}

			// fmt.Printf("endIDInt %d\n", endIDInt)
			// fmt.Printf("batchEndId %d, %d\n", batchEndIDs[batchIndex], batchIndex)

			if endIDInt == batchEndIDs[batchIndex] {

				foundStartID = false

				// Print the final block ID in this batch process
				if endBlockID != currentBlockID {
					fmt.Printf("block ID mismatch: start ID %s, end ID %s\n", currentBlockID, endBlockID)
				} else {
					fmt.Printf("The final processed block ID in this batch is %s\n", currentBlockID)
				}

				// Get the current memory usage
				memoryUsage, err := GetMemoryUsage()
				if err != nil {
					fmt.Println("Get memory usage Error:", err)
				}

				fmt.Printf("Current memory usage: %d bytes (%.2f GiB)\n", memoryUsage, float64(memoryUsage)/1024/1024/1024)
			}
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
func initializeBitSequences(numKeys int) [][]byte {
	bitSequences := make([][]byte, numKeys)
	for i := range bitSequences {
		bitSequences[i] = []byte{}
	}
	return bitSequences
}

// processLine updates the bit sequences for the current line
func processLine(line string, keys []string, keyIndexMap map[string]int, bitSequences [][]byte, lineIndex int) {
	for i := range bitSequences {
		ensureBitCapacity(&bitSequences[i], lineIndex)
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
			setBit(&bitSequences[idx], lineIndex)
		}
	}
}

// computePearsonCoefficients calculates the Pearson coefficients for all key pairs
func computePearsonCoefficients(bitSequences [][]byte, numBits int) []float64 {
	numKeys := len(bitSequences)
	pearsonResults := make([]float64, numKeys*numKeys)

	for i := 0; i < numKeys; i++ {
		for j := 0; j < numKeys; j++ {
			if i == j {
				pearsonResults[i*numKeys+j] = 1.0 // Self-correlation
				continue
			}
			pearsonResults[i*numKeys+j] = calculatePearson(bitSequences[i], bitSequences[j], numBits)
		}
	}

	return pearsonResults
}

// calculatePearson computes the Pearson correlation coefficient for two bit sequences
func calculatePearson(x, y []byte, numBits int) float64 {
	n := numBits
	var sumX, sumY, sumXY, sumX2, sumY2 float64

	for i := 0; i < n; i++ {
		bx := getBit(x, i)
		by := getBit(y, i)
		sumX += float64(bx)
		sumY += float64(by)
		sumXY += float64(bx * by)
		sumX2 += float64(bx * bx)
		sumY2 += float64(by * by)
	}

	numerator := float64(n)*sumXY - sumX*sumY
	denominator := math.Sqrt((float64(n)*sumX2 - sumX*sumX) * (float64(n)*sumY2 - sumY*sumY))

	if denominator == 0 {
		return 0.0
	}
	return numerator / denominator
}

// Bit manipulation utility functions

func ensureBitCapacity(arr *[]byte, bitIndex int) {
	byteIndex := bitIndex / 8
	if len(*arr) <= byteIndex {
		*arr = append(*arr, make([]byte, byteIndex-len(*arr)+1)...)
	}
}

func setBit(arr *[]byte, bitIndex int) {
	byteIndex := bitIndex / 8
	bitOffset := bitIndex % 8
	(*arr)[byteIndex] |= 1 << bitOffset
}

func getBit(arr []byte, bitIndex int) int {
	byteIndex := bitIndex / 8
	bitOffset := bitIndex % 8
	if byteIndex >= len(arr) {
		return 0
	}
	return int((arr[byteIndex] >> bitOffset) & 1)
}

func GetMemoryUsage() (uint64, error) {
	// Read the process statm file
	pid := os.Getpid()
	statmFile := fmt.Sprintf("/proc/%d/statm", pid)
	data, err := os.ReadFile(statmFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read statm file: %v", err)
	}

	// Parse the resident set size (in pages)
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, fmt.Errorf("invalid statm file format")
	}

	// Convert pages to bytes
	pageSize := uint64(os.Getpagesize())
	rssPages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse resident set size: %v", err)
	}

	return rssPages * pageSize, nil
}

func findIndex(target int, batchStartIDs []int) int {
	for index, id := range batchStartIDs {
		if id == target {
			return index // Return the index if the target is found
		}
	}
	return -1 // Return -1 if the target is not found
}
package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// PearsonResult stores the category-to-category Pearson coefficient results
type PearsonResult struct {
	Category1 string
	Category2 string
	Coeff     float64
}

func main() {
	// Input files
	// inputLogFile := "/home/jzhao/tst-pearson.log"
	inputLogFile := "/mnt/16T/geth-trace-withcache-merged-block-20500000-21500000"

	// Process log and compute Pearson coefficients
	processLogAndCompute(inputLogFile)
}

func processLogAndCompute(logFile string) {
	file, err := os.Open(logFile)
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

	// Open a log file for writing accumulated results
	outputFileName := fmt.Sprintf("/mnt/16T/withcache-category-pearson.log")
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		fmt.Printf("Error creating log file: %v\n", err)
		os.Exit(1)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)

	reader := bufio.NewReader(file)

	var lineCount uint64
	lineCount = 0

	var foundStartID bool
	foundStartID = false

	// for test
	// batchStartIDs := []int{20499866, 20500000}
	// batchEndIDs := []int{20499999, 20500100}
	batchStartIDs := []int{20500000}
	batchEndIDs := []int{21500000}
	var batchIndex int
	batchIndex = 0

	// Create accumulators and other necessary variables
	numPrefixes := len(withCacheHexPrefixes)
	fmt.Printf("Considered prefixes num: %d\n", numPrefixes)

	// accumulatedResults := make([]float64, numPrefixes*numPrefixes) // Accumulator for Pearson coefficients
	categoryIndexMap := make(map[string]int)
	for i, prefix := range withCacheHexPrefixes {
		categoryIndexMap[prefix.Category] = i
	}
	// Variables for per-partition processing
	// Initialize bitSequences
	bitSequences := initializeBitSequences(numPrefixes)
	fmt.Printf("Debug: Initialized bitSequences with %d categories\n", numPrefixes)

	// Variables for per-partition processing
	lineIndex := 0 // Tracks the "position" (bit index) in the current partition

	for {
		// Read the file line by line
		lineCount++

		if lineCount%10000 == 0 {
			fmt.Printf("\rProcessed %d lines", lineCount)
		}

		line, err := reader.ReadString('\n')

		if err != nil { // End of file
			if err == io.EOF && line == "" {
				break
			}
		}

		line = strings.TrimSpace(line)

		// Check if the line is the start of a block
		if matches := startRegex.FindStringSubmatch(line); matches != nil {
			currentBlockID = matches[1]
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
		}

		// Skip all lines until we have found the line with startID
		if !foundStartID {
			continue
		}

		// If inside a block, check for "OPType: Get" lines
		if opGetRegex.MatchString(line) {
			// Debug: Print the line being processed
			// fmt.Printf("Debug: Processing line: %s\n", line)

			// Extract key and size from line1
			matches := opGetRegex.FindStringSubmatch(line)
			if len(matches) != 3 {
				fmt.Printf("failed to parse key and size from line: %s", line)
			}
			line_key := matches[1]

			// Prepare the bit sequence (vector)
			processLine(line_key, categoryIndexMap, bitSequences, lineIndex)
			lineIndex++

			// Debug: Print the updated lineIndex
			// fmt.Printf("Debug: Updated lineIndex: %d\n", lineIndex)
		}

		// Check if the line is the end of the block
		if matches := endRegex.FindStringSubmatch(line); matches != nil {
			endBlockID := matches[1]
			if endBlockID != currentBlockID {
				fmt.Printf("block ID mismatch: start ID %s, end ID %s", currentBlockID, endBlockID)
			}

			endIDInt, err := strconv.Atoi(endBlockID)
			if err != nil {
				fmt.Println("Error converting end block ID to integer:", err)
			}

			if endIDInt == batchEndIDs[batchIndex] {
				foundStartID = false
				fmt.Printf("The final processed block ID in this batch is %s\n", currentBlockID)
			}
		}
	}

	// Add debug messages to check bitSequences before computing Pearson coefficients
	// fmt.Println("Debug: Checking bitSequences before computing Pearson coefficients")
	// for i := 0; i < numPrefixes; i++ {
	// 	fmt.Printf("Category: %s, Bit Sequence: ", withCacheHexPrefixes[i].Category)
	// 	for j := 0; j < lineIndex; j++ {
	// 		fmt.Printf("%d", getBit(bitSequences[i], j))
	// 	}
	// 	fmt.Println()
	// }

	// compute the Pearson between categories
	pearsonResults := computePearsonCoefficients(bitSequences, lineIndex)

	// write the results in log file
	for i := 0; i < numPrefixes; i++ {
		for j := 0; j < numPrefixes; j++ {
			coeff := pearsonResults[i*numPrefixes+j]
			// if coeff != 0 { // Only write non-zero coefficients
			line := fmt.Sprintf("category1: %s; category2: %s; coeff: %.6f\n", withCacheHexPrefixes[i], withCacheHexPrefixes[j], coeff)
			_, err := writer.WriteString(line)
			if err != nil {
				fmt.Printf("Error writing to log file: %v\n", err)
				return
			}
			// }
		}
	}

	// Flush the writer to ensure all data is written to the file
	err = writer.Flush()
	if err != nil {
		fmt.Printf("Error flushing data to log file: %v\n", err)
		return
	}

	fmt.Println("Pearson results written to category-pearson.log")
}

// initializeBitSequences initializes a 2D slice for bit sequences
func initializeBitSequences(numKeys int) [][]byte {
	bitSequences := make([][]byte, numKeys)
	for i := range bitSequences {
		bitSequences[i] = []byte{}
	}
	return bitSequences
}

func matchPrefix(key string) string {
	for _, prefix := range withCacheHexPrefixes {
		if strings.HasPrefix(key, prefix.Prefix) {
			fmt.Printf("Debug: Matched prefix: %s for key: %s\n", prefix.Prefix, key)
			return prefix.Category
		}
	}
	fmt.Printf("Debug: No prefix matched for key: %s\n", key)
	return "Unknown"
}

func processLine(line_key string, categoryIndexMap map[string]int, bitSequences [][]byte, lineIndex int) {
	for i := range bitSequences {
		ensureBitCapacity(&bitSequences[i], lineIndex)
	}

	// Debug: Print the keys found in the line
	// fmt.Printf("Debug: Line keys: %v\n", line_key)

	// Set bit for each key that appears in the current line
	// for _, key := range line_key {
	// Match the category
	category := matchPrefix(line_key)

	// Debug: Print the key and its matched category
	// fmt.Printf("Debug: Key: %s, Category: %s\n", line_key, category)

	if idx, found := categoryIndexMap[category]; found {
		// Debug: Print the bit being set
		// fmt.Printf("Debug: Setting bit for category %s at lineIndex %d\n", category, lineIndex)
		setBit(&bitSequences[idx], lineIndex)
	}
	// }
}

func setBit(arr *[]byte, bitIndex int) {
	byteIndex := bitIndex / 8
	bitOffset := bitIndex % 8

	// Debug: Print the byte index and bit offset
	// fmt.Printf("Debug: Setting bit at byteIndex %d, bitOffset %d\n", byteIndex, bitOffset)

	// Ensure the byte array has enough capacity
	if len(*arr) <= byteIndex {
		*arr = append(*arr, make([]byte, byteIndex-len(*arr)+1)...)
	}

	// Set the bit
	(*arr)[byteIndex] |= 1 << bitOffset
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

// func setBit(arr *[]byte, bitIndex int) {
// 	byteIndex := bitIndex / 8
// 	bitOffset := bitIndex % 8
// 	(*arr)[byteIndex] |= 1 << bitOffset
// }

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

// PrefixCategory represents a prefix and its corresponding category name
type PrefixCategory struct {
	Prefix   string
	Category string
}

// Define the known prefixes and their categories
var withoutCacheHexPrefixes = []PrefixCategory{
	{"536b656c65746f6e53796e63537461747573", "SkeletonSyncStatusKey"}, //
	{"5472616e73616374696f6e496e6465785461696c", "TxIndexTailKey"},    //
	{"756e636c65616e2d73687574646f776e", "UncleanShutdownKey"},        //
	{"4c617374426c6f636b", "HeadBlockKey"},                            //
	{"4c61737446696e616c697a6564", "HeadFinalizedBlockKey"},           //
	{"4c61737453746174654944", "PersistentStateIDKey"},                //
	{"69", "BloomBitsIndexPrefix"},                                    //
	{"68", "HeaderPrefix"},                                            //
	{"48", "HeaderNumberPrefix"},                                      //
	{"62", "BlockBodyPrefix"},                                         //
	{"72", "BlockReceiptsPrefix"},                                     //
	{"42", "BloomBitsPrefix"},                                         //
	{"63", "CodePrefix"},                                              //
	{"53", "SkeletonHeaderPrefix"},                                    //
	{"41", "TrieNodeAccountPrefix"},                                   //
	{"4f", "TrieNodeStoragePrefix"},                                   //
}

var withCacheHexPrefixes = []PrefixCategory{
	{"536b656c65746f6e53796e63537461747573", "SkeletonSyncStatusKey"}, //
	{"5472616e73616374696f6e496e6465785461696c", "TxIndexTailKey"},    //
	{"756e636c65616e2d73687574646f776e", "UncleanShutdownKey"},        //
	{"4c617374426c6f636b", "HeadBlockKey"},                            //
	{"4c61737446696e616c697a6564", "HeadFinalizedBlockKey"},           //
	{"4c61737453746174654944", "PersistentStateIDKey"},                //
	{"69", "BloomBitsIndexPrefix"},                                    //
	{"68", "HeaderPrefix"},                                            //
	{"48", "HeaderNumberPrefix"},                                      //
	{"62", "BlockBodyPrefix"},                                         //
	{"72", "BlockReceiptsPrefix"},                                     //
	{"42", "BloomBitsPrefix"},                                         //
	{"63", "CodePrefix"},                                              //
	{"53", "SkeletonHeaderPrefix"},                                    //
	{"41", "TrieNodeAccountPrefix"},                                   //
	{"4f", "TrieNodeStoragePrefix"},                                   //
	{"61", "SnapshotAccountPrefix"},
	{"6f", "SnapshotStoragePrefix"},
}

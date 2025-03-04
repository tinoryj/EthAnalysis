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

// PearsonResult stores the category-to-category Pearson coefficient results
type PearsonResult struct {
	Category1 string
	Category2 string
	Coeff     float64
}

func main() {
	// Input files
	inputLogFile := "/home/tinoryj/geth-trace-2025-02-11-19-18-38"

	// // The block number per batch:
	// // 5 block/min; 300 block/hour; 7200 block/day
	// blockNumPerBatch := 5

	// Process log and compute Pearson coefficients
	processLogAndCompute(inputLogFile)
	// processLog1AndCompute(log1File, keys, blockNumPerBatch)

	// // Output results
	// fmt.Println("Final Accumulated Pearson Coefficients:")
	// for i, result := range accumulatedResults {
	// 	fmt.Printf("%s - %s: %.6f\n", result.Key1, result.Key2, result.Coeff)
	// 	if (i+1)%10 == 0 {
	// 		fmt.Println()
	// 	}
	// }
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
	outputFileName := fmt.Sprintf("/mnt/sn640/category-pearson.log")
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
	batchEndIDs := []int{20759721}
	var batchIndex int
	batchIndex = 0

	// Create accumulators and other necessary variables
	numPrefixes := len(hexPrefixes)
	fmt.Printf("Considered prefixes num: %d\n", numPrefixes)

	accumulatedResults := make([]float64, numPrefixes*numPrefixes) // Accumulator for Pearson coefficients
	prefixIndexMap := make(map[string]int)
	for i, prefix := range hexPrefixes {
		prefixIndexMap[prefix] = i
	}
	// Variables for per-partition processing
	bitSequences := initializeBitSequences(numPrefixes)
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

			// fmt.Printf("in start\n")

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
			// fmt.Printf("in get\n")

			// prepare the bit sequence (vector)
			processLine(line, keys, keyIndexMap, bitSequences, lineIndex)
			lineIndex++
		}

		// Check if the line is the end of the block
		if matches := endRegex.FindStringSubmatch(line); matches != nil {
			// fmt.Printf("in end\n")

			// Verify the block ID matches
			endBlockID := matches[1]
			if endBlockID != currentBlockID {
				fmt.Printf("block ID mismatch: start ID %s, end ID %s", currentBlockID, endBlockID)
			}

			blockCounter++
			if blockCounter == blockNumPerBatch {
				// Compute Pearson coefficients for the current batch
				pearsonResults := computePearsonCoefficients(bitSequences, lineIndex)

				// Accumulate results
				for i := range pearsonResults {
					r_sequre := pearsonResults[i] * pearsonResults[i]
					accumulatedResults[i] += r_sequre
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

	// // Convert accumulated results to PearsonResult format
	// var results []PearsonResult
	// for i := 0; i < numKeys; i++ {
	// 	for j := 0; j < numKeys; j++ {
	// 		results = append(results, PearsonResult{
	// 			Key1:  keys[i],
	// 			Key2:  keys[j],
	// 			Coeff: accumulatedResults[i*numKeys+j],
	// 		})
	// 	}
	// }

	// Write accumulated results directly to the log file
	for i := 0; i < numKeys; i++ {
		for j := 0; j < numKeys; j++ {
			coeff := accumulatedResults[i*numKeys+j]
			if coeff != 0 { // Only write non-zero coefficients
				line := fmt.Sprintf("key1: %s; key2: %s; coeff: %.6f\n", keys[i], keys[j], coeff)
				_, err := writer.WriteString(line)
				if err != nil {
					fmt.Printf("Error writing to log file: %v\n", err)
					return
				}
			}
		}
	}

	// Flush the writer to ensure all data is written to the file
	err = writer.Flush()
	if err != nil {
		fmt.Printf("Error flushing data to log file: %v\n", err)
		return
	}

	fmt.Println("Accumulated results written to accumulated_results.log")
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

func matchPrefix(key string) string {
	for _, prefix := range hexPrefixes {
		if strings.HasPrefix(key, prefix.Prefix) {
			// fmt.Print("Matched prefix: ", prefix.Prefix, " for key: ", key, "\n")
			return prefix.Category
		}
	}
	return "Unknown"
}

// ParseLineForKeyPairCategories parses a log line containing a key pair and returns the categories of the two keys
func ParseLineForKeyPairCategories(line string) (string, string, bool) {
	// Regex to parse the line format
	re := regexp.MustCompile(`key: ([a-fA-F0-9\-]+);([a-fA-F0-9\-]+); Freq: (\d+); Blocks: ([\d;]+)`)
	matches := re.FindStringSubmatch(line)
	if matches == nil {
		return "", "", false
	}

	// Extract the key pair
	key1 := matches[1]
	key2 := matches[2]

	// Match the prefixes for the two keys and get their categories
	category1 := matchPrefix(key1)
	category2 := matchPrefix(key2)

	return category1, category2, true
}

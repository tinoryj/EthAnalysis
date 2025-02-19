package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// PairInfo stores the frequency and the list of BlockIDs where the pair appears
type PairInfo struct {
	Frequency int
	BlockIDs  string // BlockIDs are stored as a semicolon-separated string
}

func ProcessLogBatch(inputFile string, distance int) error {
	// Open the input log file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
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

	logname := strings.ReplaceAll(inputFile, "/", "")
	outputFileName := fmt.Sprintf("rawFreq-Dist%d-%s.log", distance, logname)
	outputFile, err = os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	// Global frequency map to store results across all blocks
	globalFrequencyMap := make(map[string]PairInfo)
	var opGetLines []string // Slice to store "OPType: Get" lines within the current block

	// Create a buffered reader
	reader := bufio.NewReader(file)

	for {
		// Read the file line by line
		line, err := reader.ReadString('\n')
		if err != nil {
			// If we reach the end of the file, break the loop
			break
		}

		// Check if the line is the start of a block
		if matches := startRegex.FindStringSubmatch(line); matches != nil {
			// Extract the block ID
			currentBlockID = matches[1]
			opGetLines = nil // Reset the slice for the new block
		}

		// If inside a block, check for "OPType: Get" lines
		if opGetRegex.MatchString(line) {
			opGetLines = append(opGetLines, line) // Store the line for frequency calculation

			// Update the global frequency map
			if len(opGetLines) > distance+1 {
				// Get the two lines at the specified distance
				line1 := opGetLines[len(opGetLines)-distance-2]
				line2 := opGetLines[len(opGetLines)-1]

				// Extract key and size from line1
				matches1 := opGetRegex.FindStringSubmatch(line1)
				if len(matches1) != 3 {
					return fmt.Errorf("failed to parse key and size from line: %s", line1)
				}
				key1 := matches1[1]
				size1 := matches1[2]

				// Extract key and size from line2
				matches2 := opGetRegex.FindStringSubmatch(line2)
				if len(matches2) != 3 {
					return fmt.Errorf("failed to parse key and size from line: %s", line2)
				}
				key2 := matches2[1]
				size2 := matches2[2]

				// Create a unique key for the pair (order-independent)
				pairKey := fmt.Sprintf("%s-%s;%s-%s", key1, size1, key2, size2)
				if key1 > key2 {
					pairKey = fmt.Sprintf("%s-%s;%s-%s", key2, size2, key1, size1)
				}

				// Update the frequency and BlockID list for this pair
				if pairInfo, exists := globalFrequencyMap[pairKey]; exists {
					// If the pair already exists, update the frequency and BlockID list
					pairInfo.Frequency++
					if !strings.Contains(pairInfo.BlockIDs, currentBlockID) {
						pairInfo.BlockIDs += ";" + currentBlockID
					}
					globalFrequencyMap[pairKey] = pairInfo
				} else {
					// If the pair does not exist, create a new entry
					globalFrequencyMap[pairKey] = PairInfo{
						Frequency: 1,
						BlockIDs:  currentBlockID,
					}
				}
			}
		}

		// Check if the line is the end of the block
		if matches := endRegex.FindStringSubmatch(line); matches != nil {
			if outputFile == nil {
				return fmt.Errorf("the raw-freq.log is not open\n")
			}

			// Verify the block ID matches
			endBlockID := matches[1]
			if endBlockID != currentBlockID {
				return fmt.Errorf("block ID mismatch: start ID %s, end ID %s", currentBlockID, endBlockID)
			}

			// if endBlockID ==

		}
	}

	// Write the global frequency results to the output file at the end of processing
	for pairKey, pairInfo := range globalFrequencyMap {
		// Only write the log if the frequency is greater than 1
		if pairInfo.Frequency > 1 {
			_, err := outputFile.WriteString(fmt.Sprintf("key: %s; Freq: %d; Blocks: %s\n", pairKey, pairInfo.Frequency, pairInfo.BlockIDs))
			if err != nil {
				return fmt.Errorf("failed to write to output file: %v", err)
			}
		}
	}

	return nil
}

func main() {
	// Example usage
	logFile := "/home/jzhao/geth-trunc-tst"
	distance := 0 // Example: distance of 0 (adjacent lines)

	err := ProcessLogBatch(logFile, distance)
	if err != nil {
		fmt.Println("Error:", err)
	}
}

package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
)

func FilterUpdate(inputFile string, outputFile string) error {

	fmt.Printf("Processing %s\n", inputFile)
	fmt.Printf("Output file: %s\n", outputFile)

	// Open the input log file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()

	opUpdateRegex := regexp.MustCompile(`OPType: Update, key: ([0-9a-fA-F]+), size: (\d+)`)

	// var opUpdateLines []string // Slice to store "OPType: Update" lines within the current block

	// Create a buffered reader
	reader := bufio.NewReader(file)

	var lineCount uint64
	lineCount = 0

	for {
		line, err := reader.ReadString('\n')

		// Process the line if it's not empty (even if err != nil)
		if len(line) > 0 {
			lineCount++

			if lineCount%10000 == 0 {
				fmt.Printf("\rProcessed %d lines", lineCount)
			}

			if opUpdateRegex.MatchString(line) {
				_, writeErr := output.WriteString(line)
				if writeErr != nil {
					fmt.Printf("\nError writing to output file: %v\n", writeErr)
				}
			}
		}

		// Break only after processing the line
		if err != nil {
			break
		}
	}

	return nil
}

func main() {
	// input log files
	logFile := "/mnt/lvm_data/FAST-26-EthAnalysis/Traces/new/geth-trace-withcache-merged-filtered-block-20500000-21500000"

	// logFile := "/mnt/lvm_data/FAST-26-EthAnalysis/Traces/new/tstupdate"

	outputFile := "/mnt/lvm_data/FAST-26-EthAnalysis/Traces/new/filtered-geth-trace-withcache-merged-filtered-block-20500000-21500000"

	err := FilterUpdate(logFile, outputFile)
	if err != nil {
		fmt.Println("Error:", err)
	}

	logFile2 := "/mnt/lvm_data/FAST-26-EthAnalysis/Traces/new/geth-trace-without-cache-merged-filtered-block-20500000-21500000"

	outputFile2 := "/mnt/lvm_data/FAST-26-EthAnalysis/Traces/new/filtered-geth-trace-without-cache-merged-filtered-block-20500000-21500000"

	err = FilterUpdate(logFile2, outputFile2)
	if err != nil {
		fmt.Println("Error:", err)
	}
	fmt.Println("\nProcessing completed successfully.")
}

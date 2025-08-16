package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// ProcessLogFile processes the log file according to the specified rules
func ProcessLogFile(inputFile, outputFile string) error {
	// Open the input log file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// Create the output file
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()

	// Regex to extract the frequency from a line
	freqRegex := regexp.MustCompile(`: (\d+)$`)

	// Variables to track the new total frequency
	var newTotalFrequency int

	// Create a buffered reader
	reader := bufio.NewReader(file)
	for {
		// Read until the next newline character
		line, err := reader.ReadString('\n')
		line = strings.TrimSuffix(line, "\n") // Remove trailing newline, if any

		// Handle the end of the file (including last line without newline)
		if err != nil {
			if err.Error() == "EOF" && len(line) > 0 {
				// Process the last line if it's not empty
				if strings.Contains(line, "Total frequency") {
					_, err := output.WriteString(fmt.Sprintf("Total frequency: %d\n", newTotalFrequency))
					if err != nil {
						return fmt.Errorf("failed to write to output file: %v", err)
					}
					break
				}

				// Handle other valid lines
				if strings.Contains(line, ";") && !strings.Contains(line, "LastPivotKey") {
					// Extract the frequency
					matches := freqRegex.FindStringSubmatch(line)
					if len(matches) < 2 {
						return fmt.Errorf("failed to parse frequency from line: %s", line)
					}

					frequency, err := strconv.Atoi(matches[1])
					if err != nil {
						return fmt.Errorf("failed to convert frequency to integer: %v", err)
					}

					// Accumulate the frequency
					newTotalFrequency += frequency

					// Write the line to the output file
					_, err = output.WriteString(line + "\n")
					if err != nil {
						return fmt.Errorf("failed to write to output file: %v", err)
					}
				}
			}
			break // Exit the loop
		}

		// Check if the line contains "LastPivotKey"
		if strings.Contains(line, "LastPivotKey") {
			// Skip this line
			continue
		}

		// Check if the line contains "Total frequency"
		if strings.Contains(line, "Total frequency") {
			// Write the updated total frequency to the output file
			_, err := output.WriteString(fmt.Sprintf("Total frequency: %d\n", newTotalFrequency))
			if err != nil {
				return fmt.Errorf("failed to write to output file: %v", err)
			}
			continue
		}

		// Extract the frequency from the line
		matches := freqRegex.FindStringSubmatch(line)
		if len(matches) < 2 {
			return fmt.Errorf("failed to parse frequency from line: %s", line)
		}

		frequency, err := strconv.Atoi(matches[1])
		if err != nil {
			return fmt.Errorf("failed to convert frequency to integer: %v", err)
		}

		// Accumulate the frequency
		newTotalFrequency += frequency

		// Write the line to the output file
		_, err = output.WriteString(line + "\n")
		if err != nil {
			return fmt.Errorf("failed to write to output file: %v", err)
		}
	}

	fmt.Printf("Processed log written to %s\n", outputFile)
	fmt.Printf("New total frequency: %d\n", newTotalFrequency)
	return nil
}

func main() {
	// Define command-line flags
	inputFile := flag.String("i", "input.log", "Input log file")
	outputFile := flag.String("o", "output.log", "Output log file")

	// Parse the flags
	flag.Parse()

	// Process the log file
	err := ProcessLogFile(*inputFile, *outputFile)
	if err != nil {
		fmt.Println("Error:", err)
	}
}

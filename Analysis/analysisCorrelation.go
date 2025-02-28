package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// PairInfo stores the frequency and the list of BlockIDs where the pair appears
type PairInfo struct {
	Frequency int
	BlockIDs  string // BlockIDs are stored as a semicolon-separated string
}

// MergeLogFiles merges frequency information from 4 log files into a global map and writes the results to an output file
func MergeLogFiles(logFiles []string, outputFile string) error {
	// Global map to store merged frequency information
	globalMap := make(map[string]PairInfo)

	// Regex to extract key pairs, frequency, and BlockIDs
	re := regexp.MustCompile(`key: ([a-fA-F0-9]+-[0-9]+);([a-fA-F0-9]+-[0-9]+); Freq: (\d+); Blocks: ([0-9;]+)`)

	for _, logFile := range logFiles {
		// Open the log file
		file, err := os.Open(logFile)
		if err != nil {
			return fmt.Errorf("failed to open log file %s: %v", logFile, err)
		}
		defer file.Close()

		// Read the file line by line
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()

			// Parse the line using the regex
			matches := re.FindStringSubmatch(line)
			if len(matches) != 5 {
				return fmt.Errorf("invalid log format: %s", line)
			}

			// Extract key pair, frequency, and BlockIDs
			keyPair := fmt.Sprintf("%s;%s", matches[1], matches[2])
			frequency, err := strconv.Atoi(matches[3])
			if err != nil {
				return fmt.Errorf("failed to parse frequency: %v", err)
			}
			blockIDs := matches[4]

			// Update the global map
			if existingPairInfo, exists := globalMap[keyPair]; exists {
				// If the key pair already exists, update the frequency and BlockIDs
				existingPairInfo.Frequency += frequency
				if !strings.Contains(existingPairInfo.BlockIDs, blockIDs) {
					existingPairInfo.BlockIDs += ";" + blockIDs
				}
				globalMap[keyPair] = existingPairInfo
			} else {
				// If the key pair does not exist, create a new entry
				globalMap[keyPair] = PairInfo{
					Frequency: frequency,
					BlockIDs:  blockIDs,
				}
			}
		}

		// Check for errors during scanning
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading log file %s: %v", logFile, err)
		}
	}

	// Write the merged results to the output file
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()

	for keyPair, pairInfo := range globalMap {
		_, err := output.WriteString(fmt.Sprintf("key: %s; Freq: %d; Blocks: %s\n", keyPair, pairInfo.Frequency, pairInfo.BlockIDs))
		if err != nil {
			return fmt.Errorf("failed to write to output file: %v", err)
		}
	}

	fmt.Printf("Merged results written to %s\n", outputFile)
	return nil
}

// LogEntry represents a single log entry with its frequency and the original line
type LogEntry struct {
	Frequency int
	Line      string
}

// ParseLogLine extracts the frequency from a log line
func ParseLogLine(line string) (int, error) {
	// Regex to extract the frequency
	re := regexp.MustCompile(`Freq: (\d+)`)
	matches := re.FindStringSubmatch(line)
	if len(matches) < 2 {
		return 0, fmt.Errorf("failed to parse frequency from line: %s", line)
	}

	// Convert frequency to integer
	frequency, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, fmt.Errorf("failed to convert frequency to integer: %v", err)
	}

	return frequency, nil
}

// SortLogFile sorts the log entries by frequency in descending order, writes the sorted results to a log file, and returns the total frequency
func SortLogFile(inputFile, outputFile string) (int, error) {
	// Open the input log file
	file, err := os.Open(inputFile)
	if err != nil {
		return 0, fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// Create a buffered reader
	reader := bufio.NewReader(file)

	// Read the file line by line
	var entries []LogEntry
	var totalFrequency int
	for {
		// Read until the next newline character
		line, err := reader.ReadString('\n')
		if err != nil {
			// If we reach the end of the file, break the loop
			break
		}

		// Remove the trailing newline character
		line = strings.TrimSuffix(line, "\n")

		// Parse the frequency from the line
		frequency, err := ParseLogLine(line)
		if err != nil {
			return 0, fmt.Errorf("error parsing log line: %v", err)
		}

		// Accumulate the total frequency
		totalFrequency += frequency

		// Store the entry with its frequency
		entries = append(entries, LogEntry{
			Frequency: frequency,
			Line:      line,
		})
	}

	// Sort the entries by frequency in descending order
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Frequency > entries[j].Frequency
	})

	// Write the sorted entries to the output file
	output, err := os.Create(outputFile)
	if err != nil {
		return 0, fmt.Errorf("failed to create output file: %v", err)
	}
	defer output.Close()

	for _, entry := range entries {
		_, err := output.WriteString(entry.Line + "\n")
		if err != nil {
			return 0, fmt.Errorf("failed to write to output file: %v", err)
		}
	}

	fmt.Printf("Sorted log written to %s\n", outputFile)
	return totalFrequency, nil
}

func main() {
	// Step 1: Merge log files

	// List of log files to merge
	logFiles := []string{
		"log1.txt",
		"log2.txt",
		"log3.txt",
		"log4.txt",
	}

	// Output file to write the merged results
	outputFile := "merged_log.log"

	// Merge the log files and write the results to the output file
	err := MergeLogFiles(logFiles, outputFile)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Setp-2: Do sorting for the merged global log

	// Input and output file paths
	inputFile := "merged_log.log"
	sortedLogFile := "sorted_output.log"

	// Sort the log file and get the total frequency
	totalFrequency, err := SortLogFile(inputFile, sortedLogFile)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Step-3: Print the total frequency
	fmt.Printf("Total frequency: %d\n", totalFrequency)

	// Step-4: Get the category frequency

}

// PrefixCategory represents a prefix and its corresponding category name
type PrefixCategory struct {
	Prefix   []byte
	Category string
}

// Define the known prefixes and their categories
var prefixes = []PrefixCategory{
	{[]byte("secure-key-"), "PreimagePrefix"},
	{[]byte("ethereum-config-"), "ConfigPrefix"},
	{[]byte("ethereum-genesis-"), "GenesisPrefix"},
	{[]byte("chtRootV2-"), "ChtPrefix"},
	{[]byte("chtIndexV2-"), "ChtIndexTablePrefix"},
	{[]byte("fixedRoot-"), "FixedCommitteeRootKey"},
	{[]byte("committee-"), "SyncCommitteeKey"},
	{[]byte("SnapSyncStatus"), "SnapSyncStatusFlagKey"},
	{[]byte("SnapshotSyncStatus"), "SnapshotSyncStatusKey"},
	{[]byte("SnapshotDisabled"), "SnapshotDisabledKey"},
	{[]byte("SnapshotRoot"), "SnapshotRootKey"},
	{[]byte("SnapshotJournal"), "SnapshotJournalKey"},
	{[]byte("SnapshotGenerator"), "SnapshotGeneratorKey"},
	{[]byte("SnapshotRecovery"), "SnapshotRecoveryKey"},
	{[]byte("SkeletonSyncStatus"), "SkeletonSyncStatusKey"},
	{[]byte("TrieJournal"), "TrieJournalKey"},
	{[]byte("TransactionIndexTail"), "TxIndexTailKey"},
	{[]byte("FastTransactionLookupLimit"), "FastTxLookupLimitKey"},
	{[]byte("InvalidBlock"), "BadBlockKey"},
	{[]byte("unclean-shutdown"), "UncleanShutdownKey"},
	{[]byte("eth2-transition"), "TransitionStatusKey"},
	{[]byte("bltRoot-"), "BloomTriePrefix"},
	{[]byte("bltIndex-"), "BloomTrieIndexPrefix"},
	{[]byte("blt-"), "BloomTrieTablePrefix"},
	{[]byte("cht-"), "ChtTablePrefix"},
	{[]byte("clique-"), "CliqueSnapshotPrefix"},
	{[]byte("update-"), "BestUpdateKey"},
	{[]byte("iB"), "BloomBitsIndexPrefix"},
	{[]byte("h"), "HeaderPrefix"},
	{[]byte("t"), "HeaderTDSuffix"},
	{[]byte("n"), "HeaderHashSuffix"},
	{[]byte("H"), "HeaderNumberPrefix"},
	{[]byte("b"), "BlockBodyPrefix"},
	{[]byte("r"), "BlockReceiptsPrefix"},
	{[]byte("l"), "TxLookupPrefix"},
	{[]byte("B"), "BloomBitsPrefix"},
	{[]byte("a"), "SnapshotAccountPrefix"},
	{[]byte("o"), "SnapshotStoragePrefix"},
	{[]byte("c"), "CodePrefix"},
	{[]byte("S"), "SkeletonHeaderPrefix"},
	{[]byte("A"), "TrieNodeAccountPrefix"},
	{[]byte("O"), "TrieNodeStoragePrefix"},
	{[]byte("L"), "StateIDPrefix"},
	{[]byte("v"), "VerklePrefix"},
	{[]byte("DatabaseVersion"), "DatabaseVersionKey"},
	{[]byte("LastHeader"), "HeadHeaderKey"},
	{[]byte("LastBlock"), "HeadBlockKey"},
	{[]byte("LastFast"), "HeadFastBlockKey"},
	{[]byte("LastFinalized"), "HeadFinalizedBlockKey"},
	{[]byte("LastStateID"), "PersistentStateIDKey"},
	{[]byte("LastPivot"), "LastPivotKey"},
	{[]byte("TrieSync"), "FastTrieProgressKey"},
}

// MatchPrefix determines the category for a given key
func MatchPrefix(key []byte) string {
	for _, prefix := range prefixes {
		if strings.HasPrefix(string(key), string(prefix.Prefix)) {
			return prefix.Category
		}
	}
	return "Unknown"
}

// ParseLogLine parses a single log line and returns the OPType and key
func ParseLogLinebyCatergory(line string) (opType string, key []byte, category string, freq int, err error) {
	// Regex to extract OPType and optionally key
	// re := regexp.MustCompile(`OPType: (\w+(?: \w+)*)(?: key: ([a-fA-F0-9]+))?, size: \d+|OPType: (\w+(?: \w+)*)$`)

	pattern := `key:\s*([\w\-]+);([\w\-]+);\s*Freq:\s*(\d+);`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(line)

	// // Extract the full key, subkey1, and subkey2
	// key := matches[1] + ";" + matches[2]
	// subkey1 := matches[1]
	// subkey2 := matches[2]

	// Attempt to decode the key
	key, err = hex.DecodeString(matches[1])
	if err != nil {
		return "", nil, "noPrefix", fmt.Errorf("invalid key: %s", matches[1])
	}

	key, err = hex.DecodeString(matches[2])
	if err != nil {
		return "", nil, "noPrefix", fmt.Errorf("invalid key: %s", matches[2])
	}

	freq, err := strconv.Atoi(matches[3])
	if err != nil {
		return 0, fmt.Errorf("failed to parse frequency: %v", err)
	}

	// Determine the prefix category
	category = MatchPrefix(key)
	if category == "" {
		category = "noPrefix"
	}

	return key, category, freq, nil
}

func GetCategoryFrequency(logLines []string) (map[string]int, error) {
	// Map to store the accumulated frequencies for category pairs
	categoryFrequencyMap := make(map[string]int)

	for _, line := range logLines {
		// Parse the log line to extract the keys and their categories
		_, key1, category1, freq1, err := ParseLogLinebyCatergory(line)
		if err != nil {
			return nil, fmt.Errorf("failed to parse log line: %v", err)
		}

		_, key2, category2, freq2, err := ParseLogLinebyCatergory(line)
		if err != nil {
			return nil, fmt.Errorf("failed to parse log line: %v", err)
		}

		// Create a unique key for the category pair (order-independent)
		categoryPair := fmt.Sprintf("%s;%s", category1, category2)
		if category1 > category2 {
			categoryPair = fmt.Sprintf("%s;%s", category2, category1)
		}

		// Update the frequency for the category pair
		frequency := freq1 + freq2
		categoryFrequencyMap[categoryPair] += frequency
	}

	return categoryFrequencyMap, nil
}

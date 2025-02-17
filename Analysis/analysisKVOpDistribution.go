package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// PrefixCategory represents a prefix and its corresponding category name
type PrefixCategory struct {
	Prefix   []byte
	Category string
}

// OperationStats stores statistics for each OPType and prefix category
type OperationStats struct {
	OpTypeCount map[string]int // Key: OPType, Value: Count
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
func ParseLogLine(line string) (opType string, key []byte, category string, err error) {
	// Regex to extract OPType and optionally key
	re := regexp.MustCompile(`OPType: (\w+(?: \w+)*)(?: key: ([a-fA-F0-9]+))?, size: \d+|OPType: (\w+(?: \w+)*)$`)
	matches := re.FindStringSubmatch(line)

	// If no match or OPType is missing, classify as noPrefix
	if len(matches) < 2 || (matches[1] == "" && matches[3] == "") {
		return "", nil, "noPrefix", fmt.Errorf("Find no key when parse line: %s", line)
	}

	// Determine the OPType
	if matches[1] != "" {
		opType = matches[1]
	} else {
		opType = matches[3]
	}

	// If the key is missing, classify as noPrefix
	if matches[2] == "" {
		return opType, nil, "noPrefix", nil
	}

	// Attempt to decode the key
	key, err = hex.DecodeString(matches[2])
	if err != nil {
		return "", nil, "noPrefix", fmt.Errorf("invalid key: %s", matches[2])
	}

	// Determine the prefix category
	category = MatchPrefix(key)
	if category == "" {
		category = "noPrefix"
	}

	return opType, key, category, nil
}


// ProcessLogFile processes the log file and computes statistics by prefix category
func ProcessLogFile(filePath string, progressInterval int) (map[string]*OperationStats, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	stats := make(map[string]*OperationStats)
	lineCount := 0

	for {
		line, err := reader.ReadString('\n') // Read until newline
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading file: %w", err)
		}

		lineCount++

		// Output progress every progressInterval lines
		if lineCount%progressInterval == 0 {
			fmt.Printf("\rProcessed %d lines...\n", lineCount)
		}

		opType, _, category, err := ParseLogLine(strings.TrimSpace(line))
		if err != nil {
			fmt.Printf("Warning: %v\n", err)
			continue
		}

		// Initialize stats for the category if not already present
		if _, exists := stats[category]; !exists {
			stats[category] = &OperationStats{OpTypeCount: make(map[string]int)}
		}

		// Update the count for the OPType in the category
		stats[category].OpTypeCount[opType]++
	}

	fmt.Printf("\rProcessed a total of %d lines.\n", lineCount)

	return stats, nil
}


// PrintStats prints the statistics
func PrintStats(stats map[string]*OperationStats, outputFile *os.File) {
	for category, opStats := range stats {
		fmt.Fprintf(outputFile, "Category: %s\n", category)
		for opType, count := range opStats.OpTypeCount {
			fmt.Fprintf(outputFile, "  OPType: %s, Count: %d\n", opType, count)
		}
	}
}


func main() {
	// Replace "logFilePath" with the path to the operation log file
	logFilePath := "/mnt/sn640/Analysis/block18121461-block18620085-KV-operations.log"
	outputFilePath := "operation_count.txt"
	progressInterval := 100000
	// Process the log file
	stats, err := ProcessLogFile(logFilePath, progressInterval)
	if err != nil {
		fmt.Printf("Error processing log file: %v\n", err)
		return
	}
	// Open the output file for writing
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()

	// Print statistics to the output file
	PrintStats(stats, outputFile)
	fmt.Printf("Statistics written to %s\n", outputFilePath)
}

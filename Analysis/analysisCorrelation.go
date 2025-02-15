package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// ProcessLogFile filters logs for "OPType: Get" within each block and writes them to a new file.
func ProcessLogFile(inputFile string) error {
	// Open the input log file
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// Regex to match block start and end lines
	startRegex := regexp.MustCompile(`Processing block \(start\), ID: (\d+)`)
	endRegex := regexp.MustCompile(`Processing block \(end\), ID: (\d+)`)
	opGetRegex := regexp.MustCompile(`OPType: Get`)

	// Variables to track the current block
	var currentBlockID string
	var outputFile *os.File

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
			// If a block is already open, close it before starting a new one
			if outputFile != nil {
				outputFile.Close()
			}

			// Extract the block ID
			currentBlockID = matches[1]

			// Create a new output file for the block
			outputFileName := fmt.Sprintf("tmp-correlation-%s.log", currentBlockID)
			outputFile, err = os.Create(outputFileName)
			if err != nil {
				return fmt.Errorf("failed to create output file: %v", err)
			}

			// Write the start line to the output file
			_, err = outputFile.WriteString(line)
			if err != nil {
				return fmt.Errorf("failed to write to output file: %v", err)
			}
		}

		// If inside a block, check for "OPType: Get" lines
		if outputFile != nil && opGetRegex.MatchString(line) {
			_, err = outputFile.WriteString(line)
			if err != nil {
				return fmt.Errorf("failed to write to output file: %v", err)
			}
		}

		// Check if the line is the end of the block
		if matches := endRegex.FindStringSubmatch(line); matches != nil {
			if outputFile == nil {
				return fmt.Errorf("block end found without a corresponding start")
			}

			// Verify the block ID matches
			endBlockID := matches[1]
			if endBlockID != currentBlockID {
				return fmt.Errorf("block ID mismatch: start ID %s, end ID %s", currentBlockID, endBlockID)
			}

			// Write the end line to the output file
			_, err = outputFile.WriteString(line)
			if err != nil {
				return fmt.Errorf("failed to write to output file: %v", err)
			}

			// Close the output file for the current block
			outputFile.Close()
			// outputFile = nil
			// currentBlockID = ""

			// TODO: already get the filtered log for a block; process it before go to the next
			tmpLogName := fmt.Sprintf("tmp-correlation-%s.log", currentBlockID)
			CalculateFrequency(tmpLogName, 0)

			outputFile = nil
			currentBlockID = ""

			// TODO: delete the temp filtered log here
			err = os.Remove(tmpLogName)
			if err != nil {
				return fmt.Errorf("failed to delete temporary log file: %v", err)
			}
			fmt.Printf("Deleted temporary log file: %s\n", tmpLogName)

		}
	}

	return nil
}

// CalculateFrequency calculates the frequency of key-value pairs at a given distance,
// includes the distance in the output, and writes the results to a log file.
func CalculateFrequency(logFile string, distance int) error {
	// Open the log file
	file, err := os.Open(logFile)
	if err != nil {
		fmt.Print("OPEN FAIL\n")

		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	// Regex to extract key and size from "OPType: Get" lines
	opGetRegex := regexp.MustCompile(`OPType: Get, key: ([0-9a-fA-F]+), size: (\d+)`)
	// Regex to extract the block ID from the log file name
	blockIDRegex := regexp.MustCompile(`tmp-correlation-(\d+)\.log`)

	// Extract the block ID from the log file name
	matches := blockIDRegex.FindStringSubmatch(logFile)
	if len(matches) != 2 {
		return fmt.Errorf("failed to extract block ID from log file name")
	}
	blockID := matches[1]

	// Slice to store the original lines of "OPType: Get"
	var opGetLines []string

	// Use bufio.Reader to read the file line by line
	reader := bufio.NewReader(file)
	for {
		// Read until the next newline character
		line, err := reader.ReadString('\n')
		if err != nil {
			// Break the loop if we reach the end of the file
			break
		}

		// Remove the trailing newline character
		line = strings.TrimSuffix(line, "\n")

		// Check if the line contains "OPType: Get"
		if opGetRegex.MatchString(line) {
			opGetLines = append(opGetLines, line)
		}
	}

	// Calculate frequency of pairs at the given distance
	frequencyMap := make(map[string]int)
	var results []string

	for i := 0; i < len(opGetLines)-distance-1; i++ {
		line1 := opGetLines[i]
		line2 := opGetLines[i+distance+1]

		// Create a unique key for the pair (order-independent)
		pairKey := fmt.Sprintf("%s;%s", line1, line2)
		if line1 > line2 {
			pairKey = fmt.Sprintf("%s;%s", line2, line1)
		}

		// Increment the frequency for this pair
		frequencyMap[pairKey]++
	}

	// Format the results
	for pairKey, freq := range frequencyMap {
		// fmt.Println("Dist: %d; Freq: %d;%s", distance, freq, pairKey)
		if freq > 1 {
			results = append(results, fmt.Sprintf("Dist: %d; Freq: %d;%s", distance, freq, pairKey))
		}
	}

	// Write the results to the output log file
	outputFileName := fmt.Sprintf("Dist-%s-Block%s-freq.log", fmt.Sprintf("%02d", distance), blockID)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outputFile.Close()

	for _, result := range results {
		_, err := outputFile.WriteString(result + "\n")
		if err != nil {
			return fmt.Errorf("failed to write to output file: %v", err)
		}
	}

	fmt.Printf("Results written to %s\n", outputFileName)
	return nil
}

func main() {
	// the trunc log file for test
	inputFile := "/home/jzhao/geth-trunc-tst"
	err := ProcessLogFile(inputFile)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Log processing completed successfully.")
	}
}

var prefixes = [][]byte{
	// databaseVersionKey tracks the current database version.
	[]byte("DatabaseVersion"),

	// headHeaderKey tracks the latest known header's hash.
	[]byte("LastHeader"),

	// headBlockKey tracks the latest known full block's hash.
	[]byte("LastBlock"),

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	[]byte("LastFast"),

	// headFinalizedBlockKey tracks the latest known finalized block hash.
	[]byte("LastFinalized"),

	// persistentStateIDKey tracks the id of latest stored state (for path-based only).
	[]byte("LastStateID"),

	// lastPivotKey tracks the last pivot block used by fast sync (to reenable on sethead).
	[]byte("LastPivot"),

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	[]byte("TrieSync"),

	// snapshotDisabledKey flags that the snapshot should not be maintained due to initial sync.
	[]byte("SnapshotDisabled"),

	// SnapshotRootKey tracks the hash of the last snapshot.
	[]byte("SnapshotRoot"),

	// snapshotJournalKey tracks the in-memory diff layers across restarts.
	[]byte("SnapshotJournal"),

	// snapshotGeneratorKey tracks the snapshot generation marker across restarts.
	[]byte("SnapshotGenerator"),

	// snapshotRecoveryKey tracks the snapshot recovery marker across restarts.
	[]byte("SnapshotRecovery"),

	// snapshotSyncStatusKey tracks the snapshot sync status across restarts.
	[]byte("SnapshotSyncStatus"),

	// skeletonSyncStatusKey tracks the skeleton sync status across restarts.
	[]byte("SkeletonSyncStatus"),

	// trieJournalKey tracks the in-memory trie node layers across restarts.
	[]byte("TrieJournal"),

	// txIndexTailKey tracks the oldest block whose transactions have been indexed.
	[]byte("TransactionIndexTail"),

	// fastTxLookupLimitKey tracks the transaction lookup limit during fast sync.
	[]byte("FastTransactionLookupLimit"),

	// badBlockKey tracks the list of bad blocks seen by local.
	[]byte("InvalidBlock"),

	// uncleanShutdownKey tracks the list of local crashes.
	[]byte("unclean-shutdown"), // config prefix for the db

	// transitionStatusKey tracks the eth2 transition status.
	[]byte("eth2-transition"),

	// snapSyncStatusFlagKey flags that status of snap sync.
	[]byte("SnapSyncStatus"),

	// headerPrefix is used for header data storage.
	[]byte("h"),

	// headerTDSuffix is used for total difficulty storage.
	[]byte("t"),

	// headerHashSuffix is used for header hash storage.
	[]byte("n"),

	// headerNumberPrefix is used for header number storage.
	[]byte("H"),

	// blockBodyPrefix is used for block body storage.
	[]byte("b"),

	// blockReceiptsPrefix is used for block receipts storage.
	[]byte("r"),

	// txLookupPrefix is used for transaction lookup.
	[]byte("l"),

	// bloomBitsPrefix is used for bloom filter bits storage.
	[]byte("B"),

	// SnapshotAccountPrefix is used for snapshot account storage.
	[]byte("a"),

	// SnapshotStoragePrefix is used for snapshot storage trie values.
	[]byte("o"),

	// CodePrefix is used for account code storage.
	[]byte("c"),

	// skeletonHeaderPrefix is used for skeleton header storage.
	[]byte("S"),

	// TrieNodeAccountPrefix is used for trie node account storage.
	[]byte("A"),

	// TrieNodeStoragePrefix is used for trie node storage paths.
	[]byte("O"),

	// stateIDPrefix is used for state ID storage.
	[]byte("L"),

	// VerklePrefix is used for Verkle trie data storage.
	[]byte("v"),

	// PreimagePrefix is used for preimage data.
	[]byte("secure-key-"),

	// configPrefix is used for database configuration.
	[]byte("ethereum-config-"),

	// genesisPrefix is used for genesis state storage.
	[]byte("ethereum-genesis-"),

	// BloomBitsIndexPrefix is used for chain indexer progress tracking.
	[]byte("iB"),

	// ChtPrefix is used for CHT root storage.
	[]byte("chtRootV2-"),

	// ChtTablePrefix is used for CHT table storage.
	[]byte("cht-"),

	// ChtIndexTablePrefix is used for CHT index table storage.
	[]byte("chtIndexV2-"),

	// BloomTriePrefix is used for bloom trie root storage.
	[]byte("bltRoot-"),

	// BloomTrieTablePrefix is used for bloom trie table storage.
	[]byte("blt-"),

	// BloomTrieIndexPrefix is used for bloom trie index storage.
	[]byte("bltIndex-"),

	// CliqueSnapshotPrefix is used for clique consensus snapshot storage.
	[]byte("clique-"),

	// BestUpdateKey is used for LightClientUpdate storage.
	[]byte("update-"),

	// FixedCommitteeRootKey is used for fixed committee root hash storage.
	[]byte("fixedRoot-"),

	// SyncCommitteeKey is used for serialized committee storage.
	[]byte("committee-"),
}

func Equal(a, b []byte) bool {
	return string(a) == string(b)
}

func HasPrefix(s, prefix []byte) bool {
	return len(s) >= len(prefix) && Equal(s[0:len(prefix)], prefix)
}

func matchesPrefix(key []byte) ([]byte, bool) {
	for _, prefix := range prefixes {
		if HasPrefix(key, prefix) {
			return prefix, true
		}
	}
	return nil, false
}

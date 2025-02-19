package main

import (
	"bufio"
	"container/heap"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/tecbot/gorocksdb"
)

// PairInfo stores the frequency and the list of BlockIDs where the pair appears
type PairInfo struct {
	Frequency int
	BlockIDs  string // BlockIDs are stored as a semicolon-separated string
}

// RocksDBWrapper is a wrapper for RocksDB operations
type RocksDBWrapper struct {
	db *gorocksdb.DB
}

// NewRocksDBWrapper initializes a new RocksDB instance
func NewRocksDBWrapper(dbPath string) (*RocksDBWrapper, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open RocksDB: %v", err)
	}
	return &RocksDBWrapper{db: db}, nil
}

// Close closes the RocksDB instance
func (r *RocksDBWrapper) Close() {
	r.db.Close()
}

// UpdatePair updates the frequency and BlockIDs for a pair in RocksDB
func (r *RocksDBWrapper) UpdatePair(pairKey string, frequency int, blockIDs string) error {
	// Read the existing pair info from RocksDB
	readOpts := gorocksdb.NewDefaultReadOptions()
	slice, err := r.db.Get(readOpts, []byte(pairKey))
	if err != nil {
		return fmt.Errorf("failed to read from RocksDB: %v", err)
	}
	defer slice.Free()

	var existingPairInfo PairInfo
	if slice.Exists() {
		// Parse the existing pair info
		parts := strings.Split(string(slice.Data()), ";")
		if len(parts) != 2 {
			return fmt.Errorf("invalid pair info format in RocksDB")
		}
		freq, err := strconv.Atoi(parts[0])
		if err != nil {
			return fmt.Errorf("failed to parse frequency: %v", err)
		}
		existingPairInfo = PairInfo{
			Frequency: freq,
			BlockIDs:  parts[1],
		}
	}

	// Update the frequency and BlockIDs
	existingPairInfo.Frequency += frequency
	if !strings.Contains(existingPairInfo.BlockIDs, blockIDs) {
		existingPairInfo.BlockIDs += ";" + blockIDs
	}

	// Write the updated pair info back to RocksDB
	writeOpts := gorocksdb.NewDefaultWriteOptions()
	value := fmt.Sprintf("%d;%s", existingPairInfo.Frequency, existingPairInfo.BlockIDs)
	err = r.db.Put(writeOpts, []byte(pairKey), []byte(value))
	if err != nil {
		return fmt.Errorf("failed to write to RocksDB: %v", err)
	}

	return nil
}

// ProcessLogFile processes a log file and updates the results in RocksDB
func ProcessLogFile(logFile string, rocksDB *RocksDBWrapper) error {
	file, err := os.Open(logFile)
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")
		if len(parts) < 3 {
			return fmt.Errorf("invalid log format: %s", line)
		}

		pairKey := parts[0]
		freqPart := strings.TrimSpace(strings.Split(parts[1], ":")[1])
		frequency, err := strconv.Atoi(freqPart)
		if err != nil {
			return fmt.Errorf("failed to parse frequency: %v", err)
		}

		blockIDs := strings.TrimSpace(strings.Split(parts[2], ":")[1])

		// Update the pair in RocksDB
		err = rocksDB.UpdatePair(pairKey, frequency, blockIDs)
		if err != nil {
			return fmt.Errorf("failed to update pair in RocksDB: %v", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log file: %v", err)
	}

	return nil
}

// WriteSortedResults writes the sorted results from RocksDB to the final log file
func WriteSortedResults(rocksDB *RocksDBWrapper, outputFile string) error {
	// Open the output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Use a min-heap to sort the results by frequency
	type HeapItem struct {
		PairKey  string
		PairInfo PairInfo
	}
	minHeap := &MinHeap{}
	heap.Init(minHeap)

	// Iterate over all pairs in RocksDB
	readOpts := gorocksdb.NewDefaultReadOptions()
	it := rocksDB.db.NewIterator(readOpts)
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		pairKey := string(it.Key().Data())
		value := string(it.Value().Data())

		parts := strings.Split(value, ";")
		if len(parts) != 2 {
			return fmt.Errorf("invalid pair info format in RocksDB")
		}

		frequency, err := strconv.Atoi(parts[0])
		if err != nil {
			return fmt.Errorf("failed to parse frequency: %v", err)
		}

		blockIDs := parts[1]

		heapItem := HeapItem{
			PairKey: pairKey,
			PairInfo: PairInfo{
				Frequency: frequency,
				BlockIDs:  blockIDs,
			},
		}

		heap.Push(minHeap, heapItem)
	}

	// Write the sorted results to the output file
	for minHeap.Len() > 0 {
		heapItem := heap.Pop(minHeap).(HeapItem)
		_, err := file.WriteString(fmt.Sprintf("%s; Freq: %d; Blocks: %s\n", heapItem.PairKey, heapItem.PairInfo.Frequency, heapItem.PairInfo.BlockIDs))
		if err != nil {
			return fmt.Errorf("failed to write to output file: %v", err)
		}
	}

	return nil
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
func ParseLogLine(line string) (opType string, key []byte, category string, freq int, err error) {
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
		_, key1, category1, freq1, err := ParseLogLine(line)
		if err != nil {
			return nil, fmt.Errorf("failed to parse log line: %v", err)
		}

		_, key2, category2, freq2, err := ParseLogLine(line)
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

func main() {
	// Initialize RocksDB
	rocksDB, err := NewRocksDBWrapper("rocksdb-data")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer rocksDB.Close()

	// Process Log1
	err = ProcessLogFile("Log1", rocksDB)
	if err != nil {
		fmt.Println("Error processing Log1:", err)
		return
	}

	// Process Log2
	err = ProcessLogFile("Log2", rocksDB)
	if err != nil {
		fmt.Println("Error processing Log2:", err)
		return
	}

	// Write the sorted final results to the output file
	err = WriteSortedResults(rocksDB, "final-log.log")
	if err != nil {
		fmt.Println("Error writing final results:", err)
		return
	}

	fmt.Println("Final results written to final-log.log")
}

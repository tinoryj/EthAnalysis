package main

import (
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/cockroachdb/pebble"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
)

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

// PrefixStats store the statistics of a prefix
type PrefixStats struct {
	Count              int         // Number of KV pairs
	TotalSize          int         // Total size of all KV pairs
	MinSizeKey         int         // Min KV pir size
	MaxSizeKey         int         // Max KV pair size
	MinSizeValue       int         // Min KV pir size
	MaxSizeValue       int         // Max KV pair size
	MinSizeKV          int         // Min KV pir size
	MaxSizeKV          int         // Max KV pair size
	SizeHistogramKV    map[int]int // Histogram of KV pair sizes (bucketed), key is the lower bound of the bucket and value is the count
	SizeHistogramKey   map[int]int // Histogram of KV pair sizes (bucketed), key is the lower bound of the bucket and value is the count
	SizeHistogramValue map[int]int // Histogram of KV pair sizes (bucketed), key is the lower bound of the bucket and value is the count
	BucketWidth        int         // Width of each bucket
}

func (ps *PrefixStats) update(sizeValue int, sizeKey int) {
	ps.Count++
	ps.TotalSize += (sizeValue + sizeKey)
	if sizeValue < ps.MinSizeValue || ps.MinSizeValue == 0 {
		ps.MinSizeValue = sizeValue
	}
	if sizeValue > ps.MaxSizeValue {
		ps.MaxSizeValue = sizeValue
	}
	bucket := sizeValue / ps.BucketWidth * ps.BucketWidth
	ps.SizeHistogramValue[bucket]++
	if sizeKey < ps.MinSizeKey || ps.MinSizeKey == 0 {
		ps.MinSizeKey = sizeKey
	}
	if sizeKey > ps.MaxSizeKey {
		ps.MaxSizeKey = sizeKey
	}
	bucket = sizeKey / ps.BucketWidth * ps.BucketWidth
	ps.SizeHistogramKey[bucket]++
	sizeKV := sizeValue + sizeKey
	if sizeKV < ps.MinSizeKV || ps.MinSizeKV == 0 {
		ps.MinSizeKV = sizeKV
	}
	if sizeKV > ps.MaxSizeKV {
		ps.MaxSizeKV = sizeKV
	}
	bucket = sizeKV / ps.BucketWidth * ps.BucketWidth
	ps.SizeHistogramKV[bucket]++
}

func (ps *PrefixStats) averageSize() float64 {
	if ps.Count == 0 {
		return 0
	}
	return float64(ps.TotalSize) / float64(ps.Count)
}

func PrintSortedHistogram(outputFile *os.File, histogram map[int]int, bucketWidth int) {
	buckets := make([]int, 0, len(histogram))
	for bucket := range histogram {
		buckets = append(buckets, bucket)
	}
	sort.Ints(buckets)

	for _, bucket := range buckets {
		count := histogram[bucket]
		fmt.Fprintf(outputFile, "    Size %d - %d B: %d \n", bucket, bucket+bucketWidth-1, count)
	}
}

func PlotHistogram(prefix string, histogram map[int]int, bucketWidth int, plotTitle string, fileName string) error {
	buckets := make([]int, 0, len(histogram))
	for bucket := range histogram {
		buckets = append(buckets, bucket)
	}
	sort.Ints(buckets)
	points := make(plotter.XYs, len(buckets))
	for i, bucket := range buckets {
		points[i].X = float64(bucket)
		points[i].Y = float64(histogram[bucket])
	}

	p := plot.New()
	p.Title.Text = plotTitle
	p.X.Label.Text = "Bucket Size (Bytes)"
	p.Y.Label.Text = "Count"

	err := plotutil.AddLinePoints(p, prefix, points)
	if err != nil {
		return err
	}

	if err := p.Save(1920, 1080, fileName); err != nil {
		return err
	}
	return nil
}

func main() {
	file := "/mnt/sn640/Analysis/MainnetDB-241101/chaindata"
	db, err := pebble.Open(file, &pebble.Options{})
	if err != nil {
		log.Fatalf("Cannot open target database, err: %v\n", err)
	}
	defer db.Close()

	const bucketWidth = 10
	const outputFilePath = "pebble-output.txt"
	const progressInterval = 1000

	prefixStatsMap := make(map[string]*PrefixStats)

	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Cannot create the output file %s: %v", outputFilePath, err)
	}
	defer outputFile.Close()

	iter, err := db.NewIter(nil)
	if err != nil {
		log.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Close()

	currentCount := 0
	noPrefix := "noPrefix"
	fmt.Printf("Start processing KV pairs\n")

	for iter.First(); iter.Valid(); iter.Next() {
		currentCount++
		if currentCount%progressInterval == 0 {
			fmt.Printf("\rProcessed %d KV pairs...", currentCount)
		}
		key := iter.Key()
		valueSize := len(iter.Value())
		keySize := len(iter.Key())
		currentPrefix := ""
		if prefix, matched := matchesPrefix(key); matched {
			currentPrefix = string(prefix)
		} else {
			currentPrefix = noPrefix
		}
		if _, exists := prefixStatsMap[currentPrefix]; !exists {
			fmt.Printf("Locate New prefix: %s\n", currentPrefix)
			prefixStatsMap[currentPrefix] = &PrefixStats{
				SizeHistogramKV:    make(map[int]int),
				SizeHistogramKey:   make(map[int]int),
				SizeHistogramValue: make(map[int]int),
				BucketWidth:        bucketWidth,
			}
		}
		prefixStatsMap[currentPrefix].update(valueSize, keySize)
	}

	if err := iter.Error(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("\rProcessed %d KV pairs... Done!\n", currentCount)

	for prefix, stats := range prefixStatsMap {
		fmt.Fprintf(outputFile, "Prefix: %s\n", prefix)
		fmt.Fprintf(outputFile, "  KV pair number: %d\n", stats.Count)
		fmt.Fprintf(outputFile, "  Average KV size: %.2f\n", stats.averageSize())
		fmt.Fprintf(outputFile, "  Min size for keys: %d\n", stats.MinSizeKey)
		fmt.Fprintf(outputFile, "  Max size for keys: %d\n", stats.MaxSizeKey)
		fmt.Fprintf(outputFile, "  Key size distribution (Bucket width: %d B):\n", bucketWidth)
		PrintSortedHistogram(outputFile, stats.SizeHistogramKey, stats.BucketWidth)
		plotFileName := fmt.Sprintf("%s_key_histogram.png", prefix)
		PlotHistogram(prefix, stats.SizeHistogramKey, stats.BucketWidth, "Key Size Distribution", plotFileName)
		fmt.Fprintf(outputFile, "  Min size for values: %d\n", stats.MinSizeValue)
		fmt.Fprintf(outputFile, "  Max size for values: %d\n", stats.MaxSizeValue)
		fmt.Fprintf(outputFile, "  Value size distribution (Bucket width: %d B):\n", bucketWidth)
		PrintSortedHistogram(outputFile, stats.SizeHistogramValue, stats.BucketWidth)
		plotFileName = fmt.Sprintf("%s_value_histogram.png", prefix)
		PlotHistogram(prefix, stats.SizeHistogramValue, stats.BucketWidth, "Value Size Distribution", plotFileName)
		fmt.Fprintf(outputFile, "  Min size for KVs: %d\n", stats.MinSizeKV)
		fmt.Fprintf(outputFile, "  Max size for KVs: %d\n", stats.MaxSizeKV)
		fmt.Fprintf(outputFile, "  KV pair size distribution (Bucket width: %d B):\n", bucketWidth)
		PrintSortedHistogram(outputFile, stats.SizeHistogramKV, stats.BucketWidth)
		plotFileName = fmt.Sprintf("%s_kv_histogram.png", prefix)
		PlotHistogram(prefix, stats.SizeHistogramKV, stats.BucketWidth, "KV Pair Size Distribution", plotFileName)
		fmt.Fprintln(outputFile)
	}

	fmt.Printf("Statistics are stored to: %s\n", outputFilePath)
}

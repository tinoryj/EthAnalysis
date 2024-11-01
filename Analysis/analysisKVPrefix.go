package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

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

// Update the statistics with a new KV pair size
func (ps *PrefixStats) update(sizeValue int, sizeKey int) {
	ps.Count++
	ps.TotalSize += (sizeValue + sizeKey)
	// Update the min and max size and histogram for value
	if sizeValue < ps.MinSizeValue || ps.MinSizeValue == 0 {
		ps.MinSizeValue = sizeValue
	}
	if sizeValue > ps.MaxSizeValue {
		ps.MaxSizeValue = sizeValue
	}
	bucket := sizeValue / ps.BucketWidth * ps.BucketWidth
	ps.SizeHistogramValue[bucket]++
	// Update the min and max size and histogram for key
	if sizeKey < ps.MinSizeKey || ps.MinSizeKey == 0 {
		ps.MinSizeKey = sizeKey
	}
	if sizeKey > ps.MaxSizeKey {
		ps.MaxSizeKey = sizeKey
	}
	bucket = sizeKey / ps.BucketWidth * ps.BucketWidth
	ps.SizeHistogramKey[bucket]++
	// Update the min and max size and histogram for KV pair
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

// Calculate the average size of KV pairs
func (ps *PrefixStats) averageSize() float64 {
	if ps.Count == 0 {
		return 0
	}
	return float64(ps.TotalSize) / float64(ps.Count)
}

// Main function to analyze the KV pairs in a LevelDB database
func main() {
	// Open the LevelDB database
	db, err := leveldb.OpenFile("/mnt/sn640/Analysis/MainnetDB-241101/chaindata", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Store the statistics of each prefix, set the bucket width to 100 bytes
	const bucketWidth = 100
	const outputFilePath = "output.txt" // output file path
	const progressInterval = 1000       // output progress every 1000 KV pairs

	prefixStatsMap := make(map[string]*PrefixStats)

	// Open the output file
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		log.Fatalf("Can not create the output file %s: %v", outputFilePath, err)
	}
	defer outputFile.Close()

	// Scan all KV pairs in the database
	iter := db.NewIterator(nil, nil)
	defer iter.Release()

	currentCount := 0
	noPrefix := "noPrefix"
	fmt.Printf("Start processing KV pairs\n")

	for iter.Next() {
		currentCount++

		// Output progress
		if currentCount%progressInterval == 0 {
			fmt.Printf("\rProcessed %d KV pairs...", currentCount)
		}

		key := string(iter.Key())
		valueSize := len(iter.Value())
		keySize := len(iter.Key())

		// Extract the prefix from the key
		prefix := ""
		if idx := strings.Index(key, "-"); idx != -1 {
			prefix = key[:idx]
		} else {
			prefix = noPrefix
		}

		// Update the statistics of the prefix
		if _, exists := prefixStatsMap[prefix]; !exists {
			prefixStatsMap[prefix] = &PrefixStats{
				SizeHistogramKV:    make(map[int]int),
				SizeHistogramKey:   make(map[int]int),
				SizeHistogramValue: make(map[int]int),
				BucketWidth:        bucketWidth,
			}
		}
		prefixStatsMap[prefix].update(valueSize, keySize)
	}

	// Check for errors
	if err := iter.Error(); err != nil {
		log.Fatal(err)
	}

	// Output the final progress
	fmt.Printf("\rProcessed %d KV pairs... Done!\n", currentCount)

	// Output the statistics of each prefix
	for prefix, stats := range prefixStatsMap {
		fmt.Fprintf(outputFile, "Prefix: %s\n", prefix)
		fmt.Fprintf(outputFile, "  KV pair number: %d\n", stats.Count)
		fmt.Fprintf(outputFile, "  Average KV size: %.2f\n", stats.averageSize())
		fmt.Fprintf(outputFile, "  Min size for keys: %d\n", stats.MinSizeKey)
		fmt.Fprintf(outputFile, "  Max size for keys: %d\n", stats.MaxSizeKey)
		fmt.Fprintf(outputFile, "  Key size distribution (Bucket width: %d B):\n", bucketWidth)
		for bucket, count := range stats.SizeHistogramKey {
			fmt.Fprintf(outputFile, "    Size %d - %d B: %d \n", bucket, bucket+bucketWidth-1, count)
		}
		fmt.Fprintf(outputFile, "  Min size for values: %d\n", stats.MinSizeValue)
		fmt.Fprintf(outputFile, "  Max size for values: %d\n", stats.MaxSizeValue)
		fmt.Fprintf(outputFile, "  Value size distribution (Bucket width: %d B):\n", bucketWidth)
		for bucket, count := range stats.SizeHistogramValue {
			fmt.Fprintf(outputFile, "    Size %d - %d B: %d \n", bucket, bucket+bucketWidth-1, count)
		}
		fmt.Fprintf(outputFile, "  Min size for KVs: %d\n", stats.MinSizeKV)
		fmt.Fprintf(outputFile, "  Max size for KVs: %d\n", stats.MaxSizeKV)
		fmt.Fprintf(outputFile, "  KV pair size distribution (Bucket width: %d B):\n", bucketWidth)
		for bucket, count := range stats.SizeHistogramKV {
			fmt.Fprintf(outputFile, "    Size %d - %d B: %d \n", bucket, bucket+bucketWidth-1, count)
		}
		fmt.Fprintln(outputFile)
	}

	fmt.Printf("Statistics are stored to: %s\n", outputFilePath)
}

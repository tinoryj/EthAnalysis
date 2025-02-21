package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type OPType string

const (
	GET         OPType = "get"
	PUT         OPType = "put"
	BATCHED_PUT OPType = "batchput"
	DELETE      OPType = "delete"
	SCAN        OPType = "scan"
)

type PrefixCategory struct {
	Prefix   string
	Category string
}

type OperationStats struct {
	OpTypeCount map[string]int
}

type OperationDistribution struct {
	GetOpDistributionCount            map[string]int
	UpdateOpDistributionCount         map[string]int
	UpdateNotBatchOpDistributionCount map[string]int
	DeleteOpDistributionCount         map[string]int
	ScanOpDistributionCountRange      map[string]int
}

var (
	stats          = make(map[string]*OperationStats)
	opDistribution = make(map[string]*OperationDistribution)
	hexPrefixes    = []PrefixCategory{
		{"7365637572652d6b65792d", "PreimagePrefix"},
		{"657468657265756d2d636f6e6669672d", "ConfigPrefix"},
		{"657468657265756d2d67656e657369732d", "GenesisPrefix"},
		{"636874526f6f7456322d", "ChtPrefix"},
		{"636874496e64657856322d", "ChtIndexTablePrefix"},
		{"6669786564526f6f742d", "FixedCommitteeRootKey"},
		{"636f6d6d69747465652d", "SyncCommitteeKey"},
		{"6368742d", "ChtTablePrefix"},
		{"626c74526f6f742d", "BloomTriePrefix"},
		{"626c74496e6465782d", "BloomTrieIndexPrefix"},
		{"626c742d", "BloomTrieTablePrefix"},
		{"636c697175652d", "CliqueSnapshotPrefix"},
		{"7570646174652d", "BestUpdateKey"},
		{"536e617073686f7453796e63537461747573", "SnapshotSyncStatusKey"},
		{"536e617073686f7444697361626c6564", "SnapshotDisabledKey"},
		{"536e617073686f74526f6f74", "SnapshotRootKey"},
		{"536e617073686f744a6f75726e616c", "SnapshotJournalKey"},
		{"536e617073686f7447656e657261746f72", "SnapshotGeneratorKey"},
		{"536e617073686f745265636f76657279", "SnapshotRecoveryKey"},
		{"536b656c65746f6e53796e63537461747573", "SkeletonSyncStatusKey"},
		{"5472696553796e63", "FastTrieProgressKey"},
		{"547269654a6f75726e616c", "TrieJournalKey"},
		{"5472616e73616374696f6e496e6465785461696c", "TxIndexTailKey"},
		{"466173745472616e73616374696f6e4c6f6f6b75704c696d6974", "FastTxLookupLimitKey"},
		{"496e76616c6964426c6f636b", "BadBlockKey"},
		{"756e636c65616e2d73687574646f776e", "UncleanShutdownKey"},
		{"657468322d7472616e736974696f6e", "TransitionStatusKey"},
		{"536e617053796e63537461747573", "SnapSyncStatusFlagKey"},
		{"446174616261736556657273696f6e", "DatabaseVersionKey"},
		{"4c617374486561646572", "HeadHeaderKey"},
		{"4c617374426c6f636b", "HeadBlockKey"},
		{"4c61737446617374", "HeadFastBlockKey"},
		{"4c61737446696e616c697a6564", "HeadFinalizedBlockKey"},
		{"4c61737453746174654944", "PersistentStateIDKey"},
		{"4c6173745069766f74", "LastPivotKey"},
		{"69", "BloomBitsIndexPrefix"},
		{"68", "HeaderPrefix"},
		{"74", "HeaderTDSuffix"},
		{"6e", "HeaderHashSuffix"},
		{"48", "HeaderNumberPrefix"},
		{"62", "BlockBodyPrefix"},
		{"72", "BlockReceiptsPrefix"},
		{"6c", "TxLookupPrefix"},
		{"42", "BloomBitsPrefix"},
		{"61", "SnapshotAccountPrefix"},
		{"6f", "SnapshotStoragePrefix"},
		{"63", "CodePrefix"},
		{"53", "SkeletonHeaderPrefix"},
		{"41", "TrieNodeAccountPrefix"},
		{"4f", "TrieNodeStoragePrefix"},
		{"4c", "StateIDPrefix"},
		{"76", "VerklePrefix"},
	}

	targetDistributionCountCategory = map[string]bool{
		"PreimagePrefix":        true,
		"ConfigPrefix":          true,
		"GenesisPrefix":         true,
		"ChtPrefix":             true,
		"ChtIndexTablePrefix":   true,
		"FixedCommitteeRootKey": true,
		"SyncCommitteeKey":      true,
		"ChtTablePrefix":        true,
		"BloomTriePrefix":       true,
		"BloomTrieIndexPrefix":  true,
		"BloomTrieTablePrefix":  true,
		"CliqueSnapshotPrefix":  true,
		"BestUpdateKey":         true,
		"SnapshotSyncStatusKey": true,
		"SnapshotDisabledKey":   true,
		"SnapshotRootKey":       true,
		"SnapshotJournalKey":    true,
		"SnapshotGeneratorKey":  true,
		"SnapshotRecoveryKey":   true,
		"SkeletonSyncStatusKey": true,
		"FastTrieProgressKey":   true,
		"TrieJournalKey":        true,
		"TxIndexTailKey":        true,
		"BadBlockKey":           true,
		"UncleanShutdownKey":    true,
		"TransitionStatusKey":   true,
		"SnapSyncStatusFlagKey": true,
		"DatabaseVersionKey":    true,
		"HeadHeaderKey":         true,
		"HeadBlockKey":          true,
		"HeadFastBlockKey":      true,
		"HeadFinalizedBlockKey": true,
		"PersistentStateIDKey":  true,
		"LastPivotKey":          true,
		"BloomBitsIndexPrefix":  true,
		"HeaderPrefix":          true,
		"HeaderTDSuffix":        true,
		"HeaderHashSuffix":      true,
		"HeaderNumberPrefix":    true,
		"BlockBodyPrefix":       true,
		"BlockReceiptsPrefix":   true,
		"TxLookupPrefix":        true,
		"BloomBitsPrefix":       true,
		"SnapshotAccountPrefix": true,
		"SnapshotStoragePrefix": true,
		"CodePrefix":            true,
		"SkeletonHeaderPrefix":  true,
		"TrieNodeAccountPrefix": true,
		"TrieNodeStoragePrefix": true,
		"StateIDPrefix":         true,
		"VerklePrefix":          true,
	}
)

func matchPrefix(key string) string {
	for _, prefix := range hexPrefixes {
		if strings.HasPrefix(key, prefix.Prefix) {
			// fmt.Print("Matched prefix: ", prefix.Prefix, " for key: ", key, "\n")
			return prefix.Category
		}
	}
	return "Unknown"
}

func parseLogLine(line string) (string, string, string, bool) {
	re := regexp.MustCompile(`OPType: (\w+(?: \w+)*), (?:key: ([a-fA-F0-9]+)|prefix: ([a-fA-F0-9]+))?`)
	matches := re.FindStringSubmatch(line)
	if matches == nil {
		return "", "", "", false
	}

	opType := matches[1]
	var key, category string

	// Always set key, preferring the 'key' value, and fallback to 'prefix'
	if matches[2] != "" {
		key = matches[2]
		category = matchPrefix(key) // You can implement matchPrefix as you did before
	} else if matches[3] != "" {
		key = matches[3]
		category = matchPrefix(key)
	} else {
		key = "" // Or whatever default value you want if neither is found
		category = "noPrefix"
	}

	return opType, category, key, true
}

func processLogFile(filePath string, progressInterval, targetProcessingCount, startBlockNumber, endBlockNumber uint64) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Sprintf("Failed to open file: %s", filePath))
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lineCount uint64
	if targetProcessingCount == 0 {
		fmt.Println("Target processing count is 0, process the whole log file")
	} else {
		fmt.Println("Target processing count is", targetProcessingCount)
	}

	start := time.Now()
	lineCount = 0
	var currentBlockID uint64
	for {
		line, err := reader.ReadString('\n') // Read until newline
		if err != nil {
			if err == io.EOF {
				fmt.Println("End of file reached")
				break
			}
			fmt.Println("Error reading file:", err)
			return
		}
		lineCount++
		// fmt.Println("Reading line:", line)
		// Loop until find a line that contains "Processing block"
		if strings.Contains(line, "Processing block (start)") {
			re := regexp.MustCompile(`ID:\s*(\d+)`)
			matches := re.FindStringSubmatch(line)
			if len(matches) > 1 {
				id, err := strconv.Atoi(matches[1]) // 将字符串转换为整数
				if err == nil {
					// fmt.Println("May skip ID:", id)
					if id >= int(startBlockNumber) {
						fmt.Println("Found the first block that is larger than (", startBlockNumber, "), start processing")
						currentBlockID = uint64(id)
						break
					}
				} else {
					fmt.Println("Error converting ID to integer:", err)
				}
			} else {
				fmt.Println("ID not found")
			}
		}
	}

	fmt.Print("Skip first ", lineCount, " lines to locate the first block (ID>=", startBlockNumber, ") before processing\n")

	for {
		line, err := reader.ReadString('\n') // Read until newline
		if err != nil {
			if err == io.EOF {
				fmt.Println("End of file reached")
				break
			}
			fmt.Println("Error reading file:", err)
			return
		}

		lineCount++
		if lineCount == targetProcessingCount && targetProcessingCount != 0 {
			fmt.Println("Processed", targetProcessingCount, "lines, stop processing")
			break
		}

		if lineCount%progressInterval == 0 {
			elapsed := time.Since(start).Seconds()
			fmt.Printf("\rProcessed %d lines, current block ID: %d, elapsed time: %.2fs", lineCount, currentBlockID, elapsed)
		}

		opType, category, key, parsed := parseLogLine(line)
		if !parsed {
			// Check if this is a block number line
			if strings.Contains(line, "Processing block") {
				re := regexp.MustCompile(`ID:\s*(\d+)`)
				matches := re.FindStringSubmatch(line)
				if len(matches) > 1 {
					id, err := strconv.Atoi(matches[1])
					if err == nil {
						currentBlockID = uint64(id)
						if id > int(endBlockNumber) {
							fmt.Println("Found the last block that is smaller than (", endBlockNumber, "), stop processing")
							break
						}
					} else {
						fmt.Println("Error converting ID to integer:", err)
					}
				}
			}
			continue
		}

		// Update stats
		if _, exists := stats[category]; !exists {
			stats[category] = &OperationStats{OpTypeCount: make(map[string]int)}
		}
		stats[category].OpTypeCount[opType]++

		// Update operation distribution
		if _, exists := opDistribution[category]; !exists {
			opDistribution[category] = &OperationDistribution{
				GetOpDistributionCount:            make(map[string]int),
				UpdateOpDistributionCount:         make(map[string]int),
				UpdateNotBatchOpDistributionCount: make(map[string]int),
				DeleteOpDistributionCount:         make(map[string]int),
				ScanOpDistributionCountRange:      make(map[string]int),
			}
		}
		dist := opDistribution[category]
		switch opType {
		case "Get":
			dist.GetOpDistributionCount[key]++
		case "BatchPut":
			dist.UpdateOpDistributionCount[key]++
		case "Put":
			dist.UpdateNotBatchOpDistributionCount[key]++
		case "BatchDelete":
			dist.DeleteOpDistributionCount[key]++
		case "NewIterator":
			dist.ScanOpDistributionCountRange[key]++
		}
	}
	fmt.Printf("\rProcessed a total of %d lines.\n", lineCount)
}

func toString(opType OPType) string {
	return string(opType)
}

func printDistributionStats(opMap map[string]int, category string, opType OPType) {
	sortedOps := make([]struct {
		Key   string
		Count int
	}, 0, len(opMap))
	for k, v := range opMap {
		sortedOps = append(sortedOps, struct {
			Key   string
			Count int
		}{k, v})
	}
	sort.Slice(sortedOps, func(i, j int) bool {
		return sortedOps[i].Count > sortedOps[j].Count
	})

	fileName := category + "_" + toString(opType) + "_dis.txt"
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %s\n", fileName)
		return
	}
	defer file.Close()

	_, _ = file.WriteString("ID\tKey\tCount\n")
	for id, entry := range sortedOps {
		_, _ = file.WriteString(fmt.Sprintf("%d\t%s\t%d\n", id+1, entry.Key, entry.Count))
	}
}

func printStats(outputFile *os.File) {
	fmt.Fprintln(outputFile, "Count of KV operations:")
	for category, opStats := range stats {
		fmt.Fprintf(outputFile, "Category: %s\n", category)
		for opType, count := range opStats.OpTypeCount {
			fmt.Fprintf(outputFile, "  OPType: %s, Count: %d\n", opType, count)
		}
	}
	fmt.Fprintln(outputFile, "\n\nDistribution of KV operations:")
	for category, opDist := range opDistribution {
		fmt.Println("Category:", category)
		if len(opDist.GetOpDistributionCount) > 1 {
			fmt.Println("\tGet operation count:", len(opDist.GetOpDistributionCount))
			printDistributionStats(opDist.GetOpDistributionCount, category, GET)
		}
		if len(opDist.UpdateOpDistributionCount) > 1 {
			fmt.Println("\tBatched put operation count:", len(opDist.UpdateOpDistributionCount))
			printDistributionStats(opDist.UpdateOpDistributionCount, category, BATCHED_PUT)
		}
		if len(opDist.UpdateNotBatchOpDistributionCount) > 1 {
			fmt.Println("\tPut operation count:", len(opDist.UpdateNotBatchOpDistributionCount))
			printDistributionStats(opDist.UpdateNotBatchOpDistributionCount, category, PUT)
		}
		if len(opDist.DeleteOpDistributionCount) > 1 {
			fmt.Println("\tDelete operation count:", len(opDist.DeleteOpDistributionCount))
			printDistributionStats(opDist.DeleteOpDistributionCount, category, DELETE)
		}
		if len(opDist.ScanOpDistributionCountRange) > 1 {
			fmt.Println("\tScan operation count:", len(opDist.ScanOpDistributionCountRange))
			printDistributionStats(opDist.ScanOpDistributionCountRange, category, SCAN)
		}
	}
}

func signalHandler(outputFile *os.File) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		printStats(outputFile)
		fmt.Println("Statistics written to ", outputFile.Name(), " due to Ctrl+C")
		os.Exit(0)
	}()
}

func main() {
	if len(os.Args) < 6 {
		fmt.Println("Usage: program <log_file_path> <out_put_log_path> <target_processing_count> <progress_interval> <start_block_number> <end_block_number>")
		return
	}
	logFilePath := os.Args[1]
	outPutLogPath := os.Args[2]
	targetProcessingCount, _ := strconv.ParseUint(os.Args[3], 10, 64)
	progressInterval, _ := strconv.ParseUint(os.Args[4], 10, 64)
	startBlockNumber, _ := strconv.ParseUint(os.Args[5], 10, 64)
	endBlockNumber, _ := strconv.ParseUint(os.Args[6], 10, 64)
	file, err := os.Create(outPutLogPath)
	if err != nil {
		fmt.Println("Error creating output file:", outPutLogPath)
		return
	}
	defer file.Close()
	fmt.Println("Processing log file:", logFilePath, "output log file:", outPutLogPath, "start block ID:", startBlockNumber, "end block ID:", endBlockNumber)
	signalHandler(file)
	processLogFile(logFilePath, progressInterval, targetProcessingCount, startBlockNumber, endBlockNumber)
	printStats(file)
}

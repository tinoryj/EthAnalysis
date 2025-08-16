# README

## Build

To compile the whole project:

```bash
$ cd  EthAnalysis/Analysis
$ bash build.sh
```

If the compilation is successful, the executable files can be found in the `bin` folder:

```bash
# currently in the EthAnalysis/Analysis
$ ls ./bin
TODO: add here
```

## Usage

### Access Correlation Analysis

#### Pearson correlation coefficients

We compute Pearson correlation coeffcients for 18 data types in CacheTrace and 16 in BareTrace which have been read. 

```bash
# After build
$ cd bin
$ ./categoryPearson -i inputLogFilePath -o outputLogFilePath
```

Each line in the output log file is formatted as `{4f TrieNodeStoragePrefix}, {41 TrieNodeAccountPrefix}, -0.984250`, where the first two columns are the prefix and category names, and the third column is the Pearson correlation coefficient of this pair.

#### Co-accessed KV pair counts with distances

We calculate the accumulated number of co-accessed KV pairs at a given distance (defined as the number of KV pairs between the co-accessed KV pairs, e.g., zero for adjacent accesses) to determine the co-access strength.

##### Co-accessed information collection

We first collect the co-accessed information by scanning the whole input log file. 

- You should configure the input log files in `main()` of`collectCorrelation.go`:
    1.  **logFiles**: input log file paths
    2.  **outputPathPrefix**: prefix of output file path
    3.  **distanceParams**: desirable distances
    4.  **batchStartIDs, batchEndIDs**: batch start/end IDs, we split batches to avoid the OOM (note that the we keep the whole frequence map which contains all co-accessed kv pairs in memory, the map can be large).

```go
func main() {
	// input log files
	logFiles := []string{
		"inputLog1",
		"inputLog2",
		"inputLog3",
	}
    
	outputPathPrefix := "/mnt/16T/"
    
	distanceParams := []int{0, 1, 4, 16, 64, 256, 1024}

	batchStartIDs := []int{20500000, 20600000, 20759722, 20884722, 21009722, 21134724, 21259723, 21379862}
	batchEndIDs := []int{20599999, 20759721, 20884721, 21009721, 21134723, 21259722, 21379861, 21500000}

	for _, logFile := range logFiles {
		for _, distance := range distanceParams {
			err := ProcessLogBatch(logFile, distance, batchStartIDs, batchEndIDs)
			if err != nil {
				fmt.Println("Error:", err)
			}
		}
	}
}
```

- Build and run:

```bash
# After build
$ cd bin
$ ./collectDistCorrelation
```

- What you get after execution:
    - output log files, whose names are formated as `[output path prefix]rawFreq-[batch start ID]-[batch end ID]-Dist[current distance]-[input log file path string].log`, the number of output log files depends on how many batches and distance parameters are configured.
    - Each line in the output log file is formatted as as `key: 41070e080f08-6;41070e080f080c-7; Freq: 3; Blocks: 20499865;20499866;20499867`, recording the keys, co-accessed count (Freq) of the kv pairs, and also the IDs of the blocks that contain such co-accesses. 

##### Co-access information analysis

We then merge the distance correlation output log batches for each distance and sort the log entries by co-access count in descending order. 

- You should configure the distance and list of log files in `main()` of `analysisCorrelation.go`:

```go
func main() {
	// analysis distance
	distance := 64

	// List of log files to merge
	logFiles := []string{
		"/mnt/16T/rawFreqWithoutCache-20599999-Dist64-homejzhaogeth-trace-2025-02-11-19-18-38.log",
		"/mnt/16T/rawFreqWithoutCache-20759721-Dist64-homejzhaogeth-trace-2025-02-11-19-18-38.log",
		"/mnt/16T/rawFreqWithoutCache-20884721-Dist64-mnt16Tgeth-trace-2025-02-13-15-33-09.log",
		"/mnt/16T/rawFreqWithoutCache-21009721-Dist64-mnt16Tgeth-trace-2025-02-13-15-33-09.log",
		"/mnt/16T/rawFreqWithoutCache-21134723-Dist64-mnt16Tgeth-trace-2025-02-13-15-33-09.log",
		"/mnt/16T/rawFreqWithoutCache-21259722-Dist64-mnt16Tgeth-trace-2025-02-13-15-33-09.log",
		"/mnt/16T/rawFreqWithoutCache-21379861-Dist64-mnt16Tgeth-trace-2025-02-13-15-33-09.log",
		"/mnt/16T/rawFreqWithoutCache-21500000-Dist64-mnt16Tgeth-trace-2025-02-13-15-33-09.log",
	}

    // ......

}

```

- Build and run:

```bash
# After build
$ cd bin
$ ./analysisDistCorrelation
```

- What you get after execution:
    - The overall sorted results for each distance are put in `[output path prefix]freq-sorted-[distance].log`, with each line formatted as `key: 41070e080f08-6;41070e080f080c-7; Freq: 3; Blocks: 20499865;20499866;20499867`, recording the keys, co-accessed count (Freq) of the kv pairs, and also the IDs of the blocks that contain such co-accesses. 
        - The overall sorted results are also partitioned by category, where each category pair has a separated log named `Dist[distance]-[category1]-[category2]-freq.log`.
    - The sorted results for each category under a certain distance are put in  `[output path prefix]category-sorted-[distance].log`, with each line formatted as `TrieNodeStoragePrefix;TrieNodeStoragePrefix: 165448516`, where the first two columns are category names, and the third column is the accumulated co-accessed count of these two categories.


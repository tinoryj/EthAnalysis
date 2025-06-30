# An Analysis of Ethereum Workloads from a Key-Value Storage Perspective

## Introduction

Blockchains have revolutionized trust and transparency in distributed systems, yet their heavy reliance on key-value (KV) storage for managing immutable, rapidly growing data leads to performance bottlenecks due to I/O inefficiencies. In this paper, we analyze Ethereum's storage workload traces, with billions of KV operations, across four dimensions: storage overhead, KV operation distributions, read correlations, and update correlations. Our study reveals 11 key findings and provides suggestions on the design and optimization of blockchain storage.

## Prerequisites

Our analysis tool is implemented in Go and requires the following prerequisites:

- Go 1.23 or later.
- Stable Internet connection to download the required packages and libraries during the build process.
- Internet connection without a proxy for synchronizing the Ethereum and collecting the traces.

To install the required packages and libraries, run the following command:

```bash
sudo apt install -y build-essential golang
# If Go version is lower than 1.22 (required by Geth), update it.
sudo apt remove golang-go
sudo apt remove --auto-remove golang-go
wget https://go.dev/dl/go1.23.2.linux-amd64.tar.gz
sudo rm -rf /usr/local/go 
sudo tar -C /usr/local -xzf go1.23.2.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin # Add this line to ~/.bashrc or ~/.zshrc
# Check the Go version
go version
```

## Trace Collection

We use the `geth` execution client and `prysm` beacon node to synchronize the Ethereum blockchain and collect the traces. Since we have modified the `geth` client to collect the traces, we provide the modified `geth` client in the `go-ethereum-1.14.11` folder.

### Setup the `geth` client

#### Setup the running mode of the `geth` client

Modify the file `go-ethereum-1.14.11/common/globalTraceLog.go` to enable the trace collection:

```go
var targetBlockNumber uint64 = 21500000 // we will use 20500000 to 21500000 as the target block range
var shouldGlobalLogInUse bool = true
```

First, you need to modify the `targetBlockNumber` to specify the stop block number for the trace collection. Then, set the `shouldGlobalLogInUse` to `true` to enable the trace collection. In addition, before collecting the trace, you may configure the `targetBlockNumber` to specify the block number that you want to start the trace collection and synchronize the Ethereum blockchain.

#### Build the modified `geth` client

- Build the modified `geth` client:

```bash
cd go-ethereum-1.14.11
make
```

- Get the `geth` client binary:

```bash
cd go-ethereum-1.14.11/build/bin
cp geth <path to store the binary and run the client> # E.g., ethereum/execution/geth
```

### Collect the traces

#### Prepare folders for the Ethereum data and logs

You need to create the following folders to store the Ethereum data and logs:

```bash
mkdir ethereum
mkdir -p ethereum/consensus
mkdir -p ethereum/execution
```

#### Create the jwt secret

Use `Prysm` consensus client to generate the jwt secret.

```bash
cd ethereum/consensus
curl https://raw.githubusercontent.com/prysmaticlabs/prysm/master/prysm.sh --output prysm.sh && chmod +x prysm.sh
./prysm.sh beacon-chain generate-auth-secret
# The generated jwt secret is stored in the file `jwt.hex`
cp jwt.hex ../jwt.hex
```

#### Run the `geth` client

Run the `geth` client to synchronize the Ethereum blockchain and collect the traces:

- To collect the BareTrace:

```bash
cd ethereum/execution
mkdir data
export GOMAXPROCS=1 # Set the number of threads to 1 (default is the number of CPU cores)
./geth --cache 0 --cache.noprefetch --snapshot --mainnet --datadir ./data --syncmode full --http --http.api eth,net,engine,admin --authrpc.jwtsecret ../jwt.hex
# --cache 0 # Disable the cache (set size to 0)
# --snapshot # Enable KV snapshot support (by default is enabled)
# --cache.noprefetch # Disable the cache prefetching to avoid the sequence interference 
```

- To collect the CacheTrace:

```bash
cd ethereum/execution
mkdir data
export GOMAXPROCS=1 # Set the number of threads to 1 (default is the number of CPU cores)
./geth  --cache.noprefetch --mainnet --datadir ./data --syncmode full --http --http.api eth,net,engine,admin --authrpc.jwtsecret ../jwt.hex
```

#### Run the `prysm` beacon node

Then, we need to run the `prysm` beacon node to synchronize the Ethereum blockchain and collect the traces:

```bash
cd ethereum/consensus
./prysm.sh beacon-chain --datadir ./data --execution-endpoint=http://localhost:8551 --mainnet --jwt-secret=../jwt.hex --checkpoint-sync-url=https://beaconstate.info --genesis-beacon-api-url=https://beaconstate.info
```

## Trace Analysis

### Build the analysis tool

To compile the whole project:

```bash
cd analysis
./build.sh install # Install the required packages and libraries, then build the project
./build.sh # Build the project without installing the required packages and libraries (i.e., for the second time)
```

If the compilation is successful, the executable files can be found in the `bin` folder.

### Usage

#### Find update from the original KV traces

You can find the updates from the original KV traces by running the following command, which will find if a write is actually an update, and generate a log file with the updates.

```bash
# After synchronizing the Ethereum blockchain and collecting the traces, stop the `geth` client and `prysm` beacon node first
cd analysis/bin
./filterUpdate <path to the Geth KV store> <original log file path> <output log file path>
# E.g., Geth KV store: /path/to/ethereum/execution/data/geth/chaindata
```

#### KV sizes analysis

You can analyze the KV sizes of the Ethereum workloads by running the following command:

```bash
# After synchronizing the Ethereum blockchain and collecting the traces, stop the `geth` client and `prysm` beacon node first 
cd analysis/bin
./countKVSizeDistribution <path to the KV store> # E.g., /path/to/ethereum/execution/data/geth/chaindata
```

It will generate the output log file `pebble-database-KV-count.txt` with the following format:

```text
...
DataType: iB
  KV pair number: 5249
  Average KV size: 46.99
  Min size for keys: 7
  Max size for keys: 15
  Key size distribution (Bucket width: 1 B):
  Min size for values: 8
  Max size for values: 32
  Value size distribution (Bucket width: 1 B):
  Min size for KVs: 15
  Max size for KVs: 47
  KV pair size distribution (Bucket width: 1 B):
...
```

In addition to the KV size summary of each data type, the tool also provides the distribution of key sizes `<data_type>_key_histogram.txt`, value sizes `<data_type>_value_histogram.txt`, and KV pair sizes `<data_type>_kv_histogram.txt`. Each line in the histogram files is formatted as `size: count`, where `size` is the size of the key, value, or KV pair, and `count` is the number of keys, values, or KV pairs with the corresponding size.

#### Access distribution analysis

You can analyze the access distribution of the Ethereum workloads by running the following command:

```bash
# After synchronizing the Ethereum blockchain and collecting the traces, stop the `geth` client and `prysm` beacon node first
cd analysis/bin
./countOpDistribution <log_file_path> <batch_size_for_each_output> <print_progress_interval> <start_block_number> <end_block_number>
# The log_file_path should be in: /path/to/ethereum/execution/geth-trace-<year>-<month>-<day>-<hour>-<minute>-<second>
# The batch_size_for_each_output is the number of blocks for each output log file, determined by the memory size of the machine. We recommend 50000 for a machine with 64 GB of memory.
# The print_progress_interval is the interval for printing the progress; we recommend 1000.
# The start_block_number and end_block_number are the block numbers for the trace collection.
```

After running the command, the tool will generate a large number of output log files with the following format in their names:

```text
distribution-<batch_start_block_number>_<batch_end_block_number>_<data_type>_<kv_operation_type>_dis.txt
```

Here, `<batch_start_block_number>` and `<batch_end_block_number>` are the block numbers of the start and end blocks for the per-batch output. In addition to the distribution of each data type and operation type, the tool also provides the overall distribution count of each data type and operation type in a separate output log file for each batch (named `countKVDist-<batch_start_block_number>_<batch_end_block_number>`). The content of the output log files is formatted as follows:

```text
...
Category: HeaderNumberPrefix
  OPType: BatchPut, Count: 50001
  OPType: Get, Count: 25419
Category: HeadHeaderKey
  OPType: BatchPut, Count: 50001
Category: TxLookupPrefix
  OPType: BatchDelete, Count: 6763887
  OPType: BatchPut, Count: 7747780
Category: BloomBitsIndexPrefix
  OPType: Get, Count: 2368
  OPType: Put, Count: 24
Category: LastPivotKey
  OPType: Get, Count: 21548
...
```

Then, we can merge the output log files to get the overall access distribution:

```bash
cd analysis/bin
# Put the real path of the result logs (distribution-*) you want to merge into a file (e.g., named "mergeOpDistFiles.txt")
./mergeOpDist mergeOpDistFiles.txt <data_type> <operation_type>
# Put the real path of the result logs (countKVDist-*) you want to merge into a file (e.g., named "mergeOpCountFiles.txt")
./mergeOpCount mergeOpCountFiles.txt >> <output_log_file>
```

#### Access correlation analysis

We consider two access types: reads and updates.

- Co-accessed KV pair counts with distances

    You can calculate the accumulated number of co-accessed KV pairs at a given distance (defined as the number of KV pairs between the co-accessed KV pairs, e.g., zero for adjacent accesses) to determine the co-access strength.

- Co-accessed information collection
    
    You need to first collect the co-accessed information by scanning the whole input log file. You should configure the input log files in `main()` of `collectReadCorrelation.go` (for reads) or `collectUpdateCorrelation.go` (for updates):

    1.  **logFiles**: input log file paths (in case you need to split the trace for storage)
    2.  **outputPathPrefix**: prefix of output file path
    3.  **distanceParams**: desirable distances
    4.  **batchStartIDs, batchEndIDs**: batch start/end IDs, we split batches to avoid the Out-of-Memory issues (note that we keep the whole frequency map, which contains all co-accessed KV pairs in memory, the map can be large).

        ```go
        func main() {
    	    // input log files
    	    logFiles := []string{
    	    	"inputLog1",
    	    	"inputLog2",
    	    	"inputLog3",
    	    }

    	    outputPathPrefix := "<path to output>"

    	    distanceParams := []int{0, 1, 4, 16, 64, 256, 1024}

    	    batchStartIDs := []int{20500000, 20600000, 20759722, 20884722, 21009722,    21134724, 21259723, 21379862}
    	    batchEndIDs := []int{20599999, 20759721, 20884721, 21009721, 21134723,  21259722, 21379861, 21500000}

    	    for _, logFile := range logFiles {
    		    for _, distance := range distanceParams {
        			err := ProcessLogBatch(logFile, distance, batchStartIDs,    batchEndIDs)
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
        $ ./collectReadCorrelation # For reads
        $ ./collectUpdateCorrelation # For updates
        ```

    - What you get after execution:
        - output log files, whose names are formated as `[output path prefix]rawFreq-[batch start ID]-[batch end ID]-Dist[current distance]-[input log file path string].log`, the number of output log files depends on how many batches and distance parameters are configured.
        - Each line in the output log file is formatted as `key: 41070e080f08-6;41070e080f080c-7; Freq: 3; Blocks: 20499865;20499866;20499867`, recording the keys, co-accessed count (Freq) of the KV pairs, and also the IDs of the blocks that contain such co-accesses.

- Analysis of co-accesses of KV pairs

    You can then merge the distance correlation output log batches for each distance and sort the log entries by co-access count in descending order.

    - You should configure the distance and list of log files in `main()` of `analysisReadCorrelation.go` (for reads) or `analysisUpdateCorrelation.go` (for updates):

        ```go
        func main() {
        	// analysis distance
        	distance := 64

        	// List of log files to merge
        	logFiles := []string{
        		"/PATH_TO_RESULTS/rawFreqWithoutCache-20599999-Dist64-trace-2025-02-11-19-18-38.log",
	        }
            // ......
        }

        ```

    - Build and run:

        ```bash
        # After build
        cd bin
        ./analysisReadCorrelation # For reads
        ./analysisUpdateCorrelation # For updates
        ```

    - What you get after execution:
        - The overall sorted results for each distance are put in `[output path prefix]freq-sorted-[distance].log`, with each line formatted as `key: 41070e080f08-6;41070e080f080c-7; Freq: 3; Blocks: 20499865;20499866;20499867`, recording the keys, co-accessed count (Freq) of the KV pairs, and also the IDs of the blocks that contain such co-accesses.
            - The overall sorted results are also partitioned by category, where each category pair has a separate log named `Dist[distance]-[category1]-[category2]-freq.log`.
        - The sorted results for each category under a certain distance are put in  `[output path prefix]category-sorted-[distance].log`, with each line formatted as `TrieNodeStoragePrefix;TrieNodeStoragePrefix: 165448516`, where the first two columns are category names, and the third column is the accumulated co-accessed count of these two categories.

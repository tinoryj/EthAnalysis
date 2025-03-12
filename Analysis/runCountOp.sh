#!/bin/bash

if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <PATH_TO_ORIGINAL_KV_LOGS> <PATH_TO_RESULTS_DIR> <START_BLOCK_ID> <END_BLOCK_ID>"
    exit 1
fi

PATH_TO_ORIGINAL_KV_LOGS=$1
PATH_TO_RESULTS_DIR=$2
START_BLOCK_ID=$3
END_BLOCK_ID=$4

mkdir -p ./countKVDis-withcache-block-20500000-21500000
./countOpDistribution $PATH_TO_WITH_CACHE 50000 10000 20500000 21500000
mv *.txt countKVDis-withcache-block-20500000-21500000



categorySet=("PreimagePrefix"        "ConfigPrefix"          "GenesisPrefix"         "ChtPrefix"             "ChtIndexTablePrefix"   "FixedCommitteeRootKey" "SyncCommitteeKey"      "ChtTablePrefix"        "BloomTriePrefix"       "BloomTrieIndexPrefix"  "BloomTrieTablePrefix"  "CliqueSnapshotPrefix"  "BestUpdateKey"         "SnapshotSyncStatusKey" "SnapshotDisabledKey"   "SnapshotRootKey"       "SnapshotJournalKey"    "SnapshotGeneratorKey"  "SnapshotRecoveryKey"   "SkeletonSyncStatusKey" "FastTrieProgressKey"   "TrieJournalKey"        "TxIndexTailKey"        "BadBlockKey"           "UncleanShutdownKey"    "TransitionStatusKey"   "SnapSyncStatusFlagKey" "DatabaseVersionKey"    "HeadHeaderKey"         "HeadBlockKey"          "HeadFastBlockKey"      "HeadFinalizedBlockKey" "PersistentStateIDKey"  "LastPivotKey"          "BloomBitsIndexPrefix"  "HeaderPrefix"          "HeaderTDSuffix"        "HeaderHashSuffix"      "HeaderNumberPrefix"    "BlockBodyPrefix"       "BlockReceiptsPrefix"   "TxLookupPrefix"        "BloomBitsPrefix"       "SnapshotAccountPrefix" "SnapshotStoragePrefix" "CodePrefix"            "SkeletonHeaderPrefix"  "TrieNodeAccountPrefix" "TrieNodeStoragePrefix" "StateIDPrefix"         "VerklePrefix")
opTypeSet=("get" "put" "batchput" "delete" "scan")

for category in "${categorySet[@]}"; do
    for opType in "${opTypeSet[@]}"; do   
    echo "Processing $category, $opType"
    canContinue=false
        currentLogFileName="processing-${category}_${opType}.txt"
        for filePathPrefix in "${filePathPrefixSet[@]}"; do
            if [ ! -f "${LogPath}/${filePathPrefix}_${category}_${opType}_dis.txt" ]; then
                echo "File ${LogPath}/${filePathPrefix}_${category}_${opType}_dis.txt does not exist"
                continue
            fi
            echo "${LogPath}/${filePathPrefix}_${category}_${opType}_dis.txt" >> "$currentLogFileName"
            canContinue=true
        done
        if [ "$canContinue" = false ]; then
            continue
        fi
        cat "$currentLogFileName"
        ./mergeOpDist "$currentLogFileName" "$category" "$opType"
        rm "$currentLogFileName"
    done
done

if [ ! -d "mergedDistribution" ]; then
    mkdir mergedDistribution
else
    rm -r mergedDistribution
    mkdir mergedDistribution
fi

mv *.txt mergedDistribution

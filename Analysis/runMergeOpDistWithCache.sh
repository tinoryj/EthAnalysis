#!/bin/bash

LogPath="/mnt/16T/GethResults/KVOpDistWithCache"
filePathPrefixSet=(
"distribution/distribution-20500000_20550000"
"distribution/distribution-20550000_20600000"
"distribution/distribution-20600000_20650000"
"distribution/distribution-20650000_20700000"
"distribution/distribution-20700000_20750000"
"distribution/distribution-20750000_20800000"
"distribution/distribution-20800000_20850000"
"distribution/distribution-20850000_20900000"
"distribution/distribution-20900000_20950000"
"distribution/distribution-20950000_21000000"
"distribution/distribution-21000000_21050000"
"distribution/distribution-21050000_21100000"
"distribution/distribution-21100000_21150000"
"distribution/distribution-21150000_21200000"
"distribution/distribution-21200000_21250000"
"distribution/distribution-21250000_21300000"
"distribution/distribution-21300000_21350000"
"distribution/distribution-21350000_21400000"
"distribution/distribution-21400000_21450000"
"distribution/distribution-21450000_21500000"
)
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

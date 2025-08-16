#!/bin/bash

LogPath="/mnt/16T/GethResults/KVOpDistWithoutCache"
filePathPrefixSet=("countKVDis-0-block-20500000-20600000"  "countKVDis-1-block-20600000-20700000"  "countKVDis-2-block-20700000-20800000"  "countKVDis-3-block-20700000-20800000"  "countKVDis-4-block-20800000-20900000"  "countKVDis-5-block-20900000-21000000"  "countKVDis-6-block-21000000-21100000"  "countKVDis-7-block-21100000-21200000"  "countKVDis-8-block-21200000-21300000"  "countKVDis-9-block-21300000-21400000" "countKVDis-10-block-21400000-21500000")
categorySet=("PreimagePrefix"        "ConfigPrefix"          "GenesisPrefix"         "ChtPrefix"             "ChtIndexTablePrefix"   "FixedCommitteeRootKey" "SyncCommitteeKey"      "ChtTablePrefix"        "BloomTriePrefix"       "BloomTrieIndexPrefix"  "BloomTrieTablePrefix"  "CliqueSnapshotPrefix"  "BestUpdateKey"         "SnapshotSyncStatusKey" "SnapshotDisabledKey"   "SnapshotRootKey"       "SnapshotJournalKey"    "SnapshotGeneratorKey"  "SnapshotRecoveryKey"   "SkeletonSyncStatusKey" "FastTrieProgressKey"   "TrieJournalKey"        "TxIndexTailKey"        "BadBlockKey"           "UncleanShutdownKey"    "TransitionStatusKey"   "SnapSyncStatusFlagKey" "DatabaseVersionKey"    "HeadHeaderKey"         "HeadBlockKey"          "HeadFastBlockKey"      "HeadFinalizedBlockKey" "PersistentStateIDKey"  "LastPivotKey"          "BloomBitsIndexPrefix"  "HeaderPrefix"          "HeaderTDSuffix"        "HeaderHashSuffix"      "HeaderNumberPrefix"    "BlockBodyPrefix"       "BlockReceiptsPrefix"   "TxLookupPrefix"        "BloomBitsPrefix"       "SnapshotAccountPrefix" "SnapshotStoragePrefix" "CodePrefix"            "SkeletonHeaderPrefix"  "TrieNodeAccountPrefix" "TrieNodeStoragePrefix" "StateIDPrefix"         "VerklePrefix")
opTypeSet=("get" "put" "batchput" "delete" "scan")

for category in "${categorySet[@]}"; do
    for opType in "${opTypeSet[@]}"; do   
    echo "Processing $category, $opType"
    canContinue=false
        currentLogFileName="processing-${category}_${opType}.txt"
        for filePathPrefix in "${filePathPrefixSet[@]}"; do
            if [ ! -f "${LogPath}/${filePathPrefix}/${category}_${opType}_dis.txt" ]; then
                echo "File ${LogPath}/${filePathPrefix}/${category}_${opType}_dis.txt does not exist"
                continue
            fi
            echo "${LogPath}/${filePathPrefix}/${category}_${opType}_dis.txt" >> "$currentLogFileName"
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

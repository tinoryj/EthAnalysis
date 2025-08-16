#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <PATH_TO_RESULTS_DIR>"
    exit 1
fi

PATH_TO_RESULTS_DIR=$1

# Get file prefix from the results dir that start with "distribution-", cut the content after the first underscore, and sort them
filePathPrefixSet=()
for file in "$PATH_TO_RESULTS_DIR"/distribution-*; do
    if [[ -f "$file" ]]; then
        filename=$(basename "$file")
        prefix=$(echo "${filename#distribution-}" | cut -d'_' -f1-2)
        prefix="distribution-"$prefix
        echo $prefix
        filePathPrefixSet+=("$prefix")
    fi
done
mapfile -t filePathPrefixSet < <(printf "%s\n" "${filePathPrefixSet[@]}" | sort -u)

categorySet=("PreimagePrefix"        "ConfigPrefix"          "GenesisPrefix"         "ChtPrefix"             "ChtIndexTablePrefix"   "FixedCommitteeRootKey" "SyncCommitteeKey"      "ChtTablePrefix"        "BloomTriePrefix"       "BloomTrieIndexPrefix"  "BloomTrieTablePrefix"  "CliqueSnapshotPrefix"  "BestUpdateKey"         "SnapshotSyncStatusKey" "SnapshotDisabledKey"   "SnapshotRootKey"       "SnapshotJournalKey"    "SnapshotGeneratorKey"  "SnapshotRecoveryKey"   "SkeletonSyncStatusKey" "FastTrieProgressKey"   "TrieJournalKey"        "TxIndexTailKey"        "BadBlockKey"           "UncleanShutdownKey"    "TransitionStatusKey"   "SnapSyncStatusFlagKey" "DatabaseVersionKey"    "HeadHeaderKey"         "HeadBlockKey"          "HeadFastBlockKey"      "HeadFinalizedBlockKey" "PersistentStateIDKey"  "LastPivotKey"          "BloomBitsIndexPrefix"  "HeaderPrefix"          "HeaderTDSuffix"        "HeaderHashSuffix"      "HeaderNumberPrefix"    "BlockBodyPrefix"       "BlockReceiptsPrefix"   "TxLookupPrefix"        "BloomBitsPrefix"       "SnapshotAccountPrefix" "SnapshotStoragePrefix" "CodePrefix"            "SkeletonHeaderPrefix"  "TrieNodeAccountPrefix" "TrieNodeStoragePrefix" "StateIDPrefix"         "VerklePrefix")
opTypeSet=("get" "put" "batchput" "update" "delete" "scan")

for category in "${categorySet[@]}"; do
    for opType in "${opTypeSet[@]}"; do   
    echo "Processing $category, $opType"
    canContinue=false
        currentLogFileName="processing-${category}_${opType}.txt"
        for filePathPrefix in "${filePathPrefixSet[@]}"; do
            if [ ! -f "${PATH_TO_RESULTS_DIR}/${filePathPrefix}_${category}_${opType}_dis.txt" ]; then
                echo "File ${PATH_TO_RESULTS_DIR}/${filePathPrefix}_${category}_${opType}_dis.txt does not exist"
                continue
            fi
            echo "${PATH_TO_RESULTS_DIR}/${filePathPrefix}_${category}_${opType}_dis.txt" >> "$currentLogFileName"
            canContinue=true
        done
        if [ "$canContinue" = false ]; then
            continue
        fi
        cat "$currentLogFileName"
        ./bin/mergeOpDist "$currentLogFileName" "$category" "$opType"
        rm "$currentLogFileName"
    done
done

if [ ! -d "${PATH_TO_RESULTS_DIR}/mergedDistribution" ]; then
    mkdir "${PATH_TO_RESULTS_DIR}/mergedDistribution"
else
    rm -r "${PATH_TO_RESULTS_DIR}/mergedDistribution"
    mkdir "${PATH_TO_RESULTS_DIR}/mergedDistribution"
fi

mv ./*.txt "${PATH_TO_RESULTS_DIR}/mergedDistribution"

# Merge the count of each operation
# Get file prefix from the results dir that start with "countDist-", cut the content after the first underscore, and sort them
overallCountfilePathPrefixSet=()
for file in "$PATH_TO_RESULTS_DIR"/countKVDist-*; do
    if [[ -f "$file" ]]; then
        filename=$(basename "$file")
        prefix=$(echo "${filename#countKVDist-}" | cut -d'_' -f1-2)
        prefix="countKVDist-"$prefix
        echo $prefix
        overallCountfilePathPrefixSet+=("$prefix")
    fi
done

mapfile -t overallCountfilePathPrefixSet < <(printf "%s\n" "${overallCountfilePathPrefixSet[@]}" | sort -u)
for overallCountfilePathPrefix in "${overallCountfilePathPrefixSet[@]}"; do
    if [ ! -f "${PATH_TO_RESULTS_DIR}/${overallCountfilePathPrefix}_count.txt" ]; then
        echo "File ${PATH_TO_RESULTS_DIR}/${overallCountfilePathPrefix}_count.txt does not exist"
        continue
    fi
    echo "${PATH_TO_RESULTS_DIR}/${overallCountfilePathPrefix}_count.txt" >> "processing-count.txt"
done

./bin/mergeOpCount "processing-count.txt" > "${PATH_TO_RESULTS_DIR}/mergedDistribution/mergedCount.txt"
rm "processing-count.txt"
echo "Done"
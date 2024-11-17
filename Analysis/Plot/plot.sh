#!/bin/bash

expID=$1

if [ -z "$expID" ]; then
    echo "Usage: $0 <expID>"
    exit 1
fi

# Main prefixes:
# "l" "TxLookupPrefix"
# "a" "SnapshotAccountPrefix"
# "o" "SnapshotStoragePrefix"
# "A" "TrieNodeAccountPrefix"
# "O" "TrieNodeStoragePrefix"

if [ "${expID}" == "1" ]; then
    # Exp#1 KV size distribution
    echo "Plot Exp#1 KV size distribution"
    # Correctly define an array with prefix and note separated into individual elements
    prefixToNoteSet=("a SnapshotAccountPrefix" "o SnapshotStoragePrefix" "A TrieNodeAccountPrefix" "O TrieNodeStoragePrefix")
    # Iterate over the array and split prefix and note correctly
    for prefixToNote in "${prefixToNoteSet[@]}"; do
        prefix=$(echo "$prefixToNote" | awk '{print $1}')
        note=$(echo "$prefixToNote" | awk '{print $2}')
        ./plotSingle.sh path=./KVSize rSrc=kvSizeDistribution dataSrc=kvSize/"${prefix}"_kv_histogram target=kvSizeDistribution_"${note}"
    done
elif [ "${expID}" == "2" ]; then
    # Exp#2 KV operations
    echo "Plot Exp#2 KV operations"
    opType=${2:-"plot"}
    # Correctly define an array with prefix and note separated into individual elements
    prefixSet=("SnapshotAccountPrefix" "SnapshotStoragePrefix" "TrieNodeAccountPrefix" "TrieNodeStoragePrefix")
    typeSet=("get" "delete" "update")
    # Iterate over the array and split prefix and note correctly
    for prefix in "${prefixSet[@]}"; do
        for type in "${typeSet[@]}"; do
            if [ "${opType}" == "plot" ]; then
                ./plotSingle.sh path=./opDistribution rSrc=kvOpDistributionPlot dataSrc=opDis/"${prefix}"_"${type}"_dis target=kvOpDistribution_"${prefix}_${type}"
            elif [ "${opType}" == "fit" ]; then
                ./plotSingle.sh path=./opDistribution rSrc=kvOpDistributionCurve dataSrc=opDis/"${prefix}"_"${type}"_dis target=kvOpDistribution_"${prefix}_${type}"
            else
                echo "Invalid opType: ${opType}"
                exit 1
            fi
        done
    done
else
    echo "Invalid expID: ${expID}"
    exit 1
fi

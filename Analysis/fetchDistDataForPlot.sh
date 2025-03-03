#!/bin/bash

PATH_TO_WITHOUT_CACHE=/mnt/16T/GethResults/KVOpDistWithoutCache/mergedDistribution
PATH_TO_WITH_CACHE=/mnt/16T/GethResults/KVOpDistWithCache/mergedDistribution

TargetDataTypes=("SnapshotAccountPrefix" "SnapshotStoragePrefix" "TrieNodeAccountPrefix" "TrieNodeStoragePrefix" "TxLookupPrefix")
TargetOpTypes=("get" "put" "batchput" "delete")
TargetDrawPonitNumbers=(1000 10000 50000)

for TargetDataType in "${TargetDataTypes[@]}"; do
    for TargetOpType in "${TargetOpTypes[@]}"; do
        for TargetDrawPonitNumber in "${TargetDrawPonitNumbers[@]}"; do
            echo "Processing ${TargetDataType} ${TargetOpType} ${TargetDrawPonitNumber}"
            filePathWithoutCache="${PATH_TO_WITHOUT_CACHE}/${TargetDataType}_${TargetOpType}_without_key_dis.txt"
            filePathWithCache="${PATH_TO_WITH_CACHE}/${TargetDataType}_${TargetOpType}_without_key_dis.txt"
            if [ -f "$filePathWithCache" ]; then
                outputFileName="${TargetDataType}_${TargetOpType}_sample_${TargetDrawPonitNumber}_withCache.txt"
                python3 generateDistDataForPlot.py "${filePathWithCache}" "${TargetDrawPonitNumber}" "${outputFileName}"
            fi
            if [ -f "$filePathWithoutCache" ]; then
                outputFileName="${TargetDataType}_${TargetOpType}_sample_${TargetDrawPonitNumber}_withoutCache.txt"
                python3 generateDistDataForPlot.py "${filePathWithoutCache}" "${TargetDrawPonitNumber}" "${outputFileName}"
            fi
        done
    done
done

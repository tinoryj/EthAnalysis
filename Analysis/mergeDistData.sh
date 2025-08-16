#!/bin/bash

pathToData=./sampled

TargetDataTypes=("TrieNodeAccountPrefix" "TrieNodeStoragePrefix")
TargetOpTypes=("get" "put" "batchput" "delete")
TargetDrawPonitNumbers=(1000 10000)

for TargetDataType in "${TargetDataTypes[@]}"; do
    for TargetOpType in "${TargetOpTypes[@]}"; do
        for TargetDrawPonitNumber in "${TargetDrawPonitNumbers[@]}"; do
            echo "Processing ${TargetDataType} ${TargetOpType} ${TargetDrawPonitNumber}"
            filePathWithCache="${TargetDataType}_${TargetOpType}_sample_${TargetDrawPonitNumber}_withCache.txt"
            filePathWithoutCache="${TargetDataType}_${TargetOpType}_sample_${TargetDrawPonitNumber}_withoutCache.txt"
            filePathOutput="${TargetDataType}_${TargetOpType}_sample_${TargetDrawPonitNumber}.txt"
            # Check if the file exists
            if [ ! -f "${pathToData}/${filePathWithCache}" ]; then
                echo "${pathToData}/${filePathWithCache} does not exist"
                continue
            fi
            python3 mergeData.py "${pathToData}/${filePathWithCache}" "${pathToData}/${filePathWithoutCache}" "${filePathOutput}"
        done
    done
done

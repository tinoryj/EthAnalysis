#!/bin/bash

PATH_TO_WITHOUT_CACHE=/mnt/16T/GethResults/correlation-dist/category-merged/without-cache
PATH_TO_WITH_CACHE=/mnt/16T/GethResults/correlation-dist/category-merged/with-cache

TargetDataTypes=(
"SnapshotAccountPrefix-SnapshotAccountPrefix"
"SnapshotAccountPrefix-SnapshotStoragePrefix"
"SnapshotAccountPrefix-TrieNodeAccountPrefix"
"SnapshotAccountPrefix-TrieNodeStoragePrefix"
"SnapshotStoragePrefix-SnapshotAccountPrefix" 
"SnapshotStoragePrefix-SnapshotStoragePrefix" 
"SnapshotStoragePrefix-TrieNodeAccountPrefix" 
"SnapshotStoragePrefix-TrieNodeStoragePrefix" 
"TrieNodeAccountPrefix-SnapshotAccountPrefix" 
"TrieNodeAccountPrefix-SnapshotStoragePrefix" 
"TrieNodeAccountPrefix-TrieNodeAccountPrefix" 
"TrieNodeAccountPrefix-TrieNodeStoragePrefix" 
"TrieNodeStoragePrefix-SnapshotAccountPrefix"
"TrieNodeStoragePrefix-SnapshotStoragePrefix"
"TrieNodeStoragePrefix-TrieNodeAccountPrefix"
"TrieNodeStoragePrefix-TrieNodeStoragePrefix"
)
TargetDrawPonitNumbers=(1000)
resultsFolder="correlationData"
if [ ! -d "$resultsFolder" ]; then
    mkdir "$resultsFolder"
else
    rm -r "$resultsFolder"
    mkdir "$resultsFolder"
fi
for TargetDataType in "${TargetDataTypes[@]}"; do
        for TargetDrawPonitNumber in "${TargetDrawPonitNumbers[@]}"; do
            echo "Processing ${TargetDataType} ${TargetDrawPonitNumber}"
            filePathWithoutCache="${PATH_TO_WITHOUT_CACHE}/dist0-category/Dist0-${TargetDataType}-freq.log"
            filePathWithCache="${PATH_TO_WITH_CACHE}/withcache-dist0-category/withcache-Dist0-${TargetDataType}-freq.log"
            if [ -f "$filePathWithCache" ]; then
                outputFileName="${resultsFolder}/${TargetDataType}_sample_${TargetDrawPonitNumber}_withCache.txt"
                python3 generateCorrelationDataForPlot.py "${filePathWithCache}" "${TargetDrawPonitNumber}" "${outputFileName}"
            else
                echo "File ${filePathWithCache} does not exist"
            fi
            if [ -f "$filePathWithoutCache" ]; then
                outputFileName="${resultsFolder}/${TargetDataType}_sample_${TargetDrawPonitNumber}_withoutCache.txt"
                python3 generateCorrelationDataForPlot.py "${filePathWithoutCache}" "${TargetDrawPonitNumber}" "${outputFileName}"
            else
                echo "File ${filePathWithoutCache} does not exist"
            fi
        done
done

#!/bin/bash

pathToData=./correlationData
outputPath=./correlationDataMerged
if [ ! -d "$outputPath" ]; then
    mkdir "$outputPath"
else
    rm -r "$outputPath"
    mkdir "$outputPath"
fi

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
tempFileListWithoutCache="tempFileList_without_cache.txt"
tempFileListWithCache="tempFileList_with_cache.txt"
if [ -f "$tempFileListWithoutCache" ]; then
    rm "$tempFileListWithoutCache"
fi
if [ -f "$tempFileListWithCache" ]; then
    rm "$tempFileListWithCache"
fi
for TargetDataType in "${TargetDataTypes[@]}"; do
        for TargetDrawPonitNumber in "${TargetDrawPonitNumbers[@]}"; do
            echo "Processing ${TargetDataType} ${TargetDrawPonitNumber}"
            filePathWithCache="${TargetDataType}_sample_${TargetDrawPonitNumber}_withCache.txt"
            filePathWithoutCache="${TargetDataType}_sample_${TargetDrawPonitNumber}_withoutCache.txt"
            # Check if the file exists
            if [ ! -f "${pathToData}/${filePathWithCache}" ]; then
                echo "${pathToData}/${filePathWithCache} does not exist"
                continue
            fi
            echo "${pathToData}/${filePathWithCache}" >> "$tempFileListWithCache"
            if [ ! -f "${pathToData}/${filePathWithoutCache}" ]; then
                echo "${pathToData}/${filePathWithoutCache} does not exist"
                continue
            fi
            echo "${pathToData}/${filePathWithoutCache}" >> "$tempFileListWithoutCache"
        done
done

python3 mergeCorrelationData.py "${tempFileListWithCache}" "${outputPath}/correlation_dist_0_withCache.txt"
python3 mergeCorrelationData.py "${tempFileListWithoutCache}" "${outputPath}/correlation_dist_0_withoutCache.txt"
#!/bin/bash

PATH_TO_FIRST_PART=/home/jzhao/geth-trace-2025-02-11-19-18-38
PATH_TO_SECOND_PART=/mnt/ssd/geth-trace-2025-02-13-15-33-09
PATH_TO_WITH_CACHE=/mnt/16T/geth-trace-withcache-merged-block-20500000-21500000

target=$1
if [ "$target" == "without" ]; then
    num_blocks_in_each_run=100000
    start_block_id=20500000
    end_block_id=20600000

    for i in {0..2}; do
        echo "Run $i-th part, from block $start_block_id to $end_block_id"
        mkdir -p ./countKVDis-$i-block-$start_block_id-$end_block_id
        ./countOpDistribution $PATH_TO_FIRST_PART ./countKVDis.txt 0 10000 $start_block_id $end_block_id
        mv *.txt countKVDis-$i-block-$start_block_id-$end_block_id
        start_block_id=$((start_block_id + num_blocks_in_each_run))
        end_block_id=$((end_block_id + num_blocks_in_each_run))
    done

    start_block_id=21400000
    end_block_id=21500000

    for i in {10..10}; do
        echo "Run $i-th part"
        mkdir -p ./countKVDis-$i-block-$start_block_id-$end_block_id
        ./countOpDistribution $PATH_TO_SECOND_PART ./countKVDis.txt 0 10000 $start_block_id $end_block_id
        mv *.txt countKVDis-$i-block-$start_block_id-$end_block_id
        start_block_id=$((start_block_id + num_blocks_in_each_run))
        end_block_id=$((end_block_id + num_blocks_in_each_run))
    done

else 

    num_blocks_in_each_run=250000
    start_block_id=20500000
    end_block_id=20750000

    for i in {0..3}; do
        echo "Run $i-th part, from block $start_block_id to $end_block_id"
        mkdir -p ./countKVDis-$i-block-$start_block_id-$end_block_id
        ./countOpDistribution $PATH_TO_WITH_CACHE ./countKVDis.txt 0 10000 $start_block_id $end_block_id
        mv *.txt countKVDis-$i-block-$start_block_id-$end_block_id
        start_block_id=$((start_block_id + num_blocks_in_each_run))
        end_block_id=$((end_block_id + num_blocks_in_each_run))
    done
fi

#!/bin/bash

PATH_TO_FIRST_PART=/home/jzhao/geth-trace-2025-02-11-19-18-38
PATH_TO_SECOND_PART=/mnt/ssd/geth-trace-2025-02-13-15-33-09

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

start_block_id=20700000
end_block_id=20800000

for i in {3..9}; do
    echo "Run $i-th part"
    mkdir -p ./countKVDis-$i-block-$start_block_id-$end_block_id
    ./countOpDistribution $PATH_TO_SECOND_PART ./countKVDis.txt 0 10000 $start_block_id $end_block_id
    mv *.txt countKVDis-$i-block-$start_block_id-$end_block_id
    start_block_id=$((start_block_id + num_blocks_in_each_run))
    end_block_id=$((end_block_id + num_blocks_in_each_run))
done

#!/bin/bash

./bin/filterUpdates /mnt/ssd/chaindata /mnt/lvm_data/FAST-26-EthAnalysis/Traces/geth-trace-withcache-merged-block-20500000-21500000 /mnt/lvm_data/FAST-26-EthAnalysis/Traces/new/geth-trace-withcache-merged-filtered-block-20500000-21500000

./bin/filterUpdates /mnt/ssd/chaindata /mnt/lvm_data/FAST-26-EthAnalysis/Traces/geth-trace-without-cache-merged-block-20500000-21500000 /mnt/lvm_data/FAST-26-EthAnalysis/Traces/new/geth-trace-without-cache-merged-filtered-block-20500000-21500000

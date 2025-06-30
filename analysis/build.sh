#!/bin/bash

ShouldInstall=$1
if [ "$ShouldInstall" == "install" ]; then
    go mod init eth
    go get github.com/syndtr/goleveldb/leveldb
    go get github.com/cockroachdb/pebble
    go get github.com/ethereum/go-ethereum/rlp
    go get github.com/syndtr/goleveldb
    go get github.com/getsentry/sentry-go@v0.27.0
    go get github.com/cockroachdb/pebble@v1.1.2
    go get golang.org/x/sys/unix
    go get golang.org/x/exp/rand
    go get github.com/cespare/xxhash/v2
    go get github.com/golang/protobuf/proto
    go get github.com/prometheus/client_model/go@v0.2.1-0.20210607210712-147c58e9608a
    go get github.com/golang/snappy
    go get gonum.org/v1/plot
    go get gonum.org/v1/plot/plotter
    go get gonum.org/v1/plot/plotutil
fi
    
if [ ! -d "bin" ]; then
    mkdir bin
else
    rm -rf bin/*
fi

# for KV size and operation distribution
go build -o bin/countKVSizeDistribution analysisKVStoragePebble.go
go build -o bin/countOpDistribution analysisOpDistributionByBatch.go
go build -o bin/mergeOpDist analysisOpDistributionMergeDistribution.go
go build -o bin/mergeOpCount analysisOpDistributionMergeCount.go
# for read correlation
go build -o bin/collectReadCorrelation collectReadCorrelation.go
go build -o bin/analysisReadCorrelation analysisReadCorrelation.go
go build -o bin/categoryPearson analysisCategoryPearson.go
# for update correlation
go build -o bin/collectUpdateCorrelation collectUpdateCorrelation.go
go build -o bin/analysisUpdateCorrelation analysisUpdateCorrelation.go
# for filter updates from the original KV traces
go build -o bin/filterUpdate filterUpdate.go
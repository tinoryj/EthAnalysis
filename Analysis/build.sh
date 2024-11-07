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
fi
go build -o db_stats_leveldb analysisKVPrefixLeveldb.go
go build -o db_stats_pebble analysisKVPrefixPebble.go

#!/bin/bash

ShouldInstall=$1
if [ "$ShouldInstall" == "install" ]; then
    go mod init eth
    go get github.com/syndtr/goleveldb/leveldb
fi
go build -o db_stats analysisKVPrefix.go

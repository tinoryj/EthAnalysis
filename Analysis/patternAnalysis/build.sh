#!/bin/bash
cpuMaxThreadNumber=$(cat /proc/cpuinfo | grep processor | wc -l)
echo "cpuMaxThreadNumber: $cpuMaxThreadNumber"

if [ -d "./build" ] || [ -d "./bin" ]; then
    rm -rf ./build ./bin
fi
mkdir -p ./build ./bin

cd ./build || exit
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$cpuMaxThreadNumber

cd .. || exit

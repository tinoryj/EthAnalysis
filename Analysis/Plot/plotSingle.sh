#!/bin/bash

path="."
rSrc="default"
dataSrc="default"
target="default"

for param in $*; do
    if [[ $(echo $param | grep "path" | wc -l) -eq 1 ]]; then
        path=$(echo $param | sed 's/path=//g')
    elif [[ $(echo $param | grep "rSrc" | wc -l) -eq 1 ]]; then
        rSrc=$(echo $param | sed 's/rSrc=//g')
    elif [[ $(echo $param | grep "dataSrc" | wc -l) -eq 1 ]]; then
        dataSrc=$(echo $param | sed 's/dataSrc=//g')
    elif [[ $(echo $param | grep "target" | wc -l) -eq 1 ]]; then
        target=$(echo $param | sed 's/target=//g')
    fi
done

Rscript $path/$rSrc.r $path/$dataSrc.txt $path/$target.pdf
# pdfcrop $path/$target.pdf $path/$target.pdf

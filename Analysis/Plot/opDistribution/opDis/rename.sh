#!/bin/bash

# 遍历当前目录下所有以 .log 结尾的文件
for file in *.log; do
    # 提取文件名前缀（去掉 .log 后缀）
    base_name="${file%.log}"
    # 重命名文件，将后缀改为 .txt
    mv "$file" "${base_name}.txt"
done

echo "所有 .log 文件已重命名为 .txt 文件。"

import pandas as pd
import sys
import os
import csv

def process_file(fileList_path, output_file_path):
    fileList = pd.read_csv(fileList_path, sep='\t', header=0)
    total_files = len(fileList)
    print(f"Total files: {total_files}")
    output_csv_writer = csv.writer(open(output_file_path, 'w'), delimiter='\t')
    output_csv_writer.writerow(['ID', 'Type', 'Count'])
    for file in fileList:
        basename = os.path.basename(file)
        basename_prefix = basename.split('_')[0]
        # Convert basename_prefix to replace - with 2
        basename_prefix = basename_prefix.replace('-', '2')
        data_in_file = pd.read_csv(file, sep='\t', header=0)
        total_rows = len(data_in_file)
        print(f"Total rows in {basename}: {total_rows}")
        for i in range(total_rows):
            output_csv_writer.writerow([data_in_file.iloc[i, :]['ID'], basename_prefix, int(data_in_file.iloc[i, :]['Count'])])

fileList_path = sys.argv[1]
output_file_path = sys.argv[2]

process_file(fileList_path, output_file_path)
import pandas as pd
import sys
import csv

def process_file(file_path_witcache, file_path_withoutcache, output_file_path):
    data_with_cache = pd.read_csv(file_path_witcache, sep='\t', header=0)
    total_rows_with_cache = len(data_with_cache)
    print(f"Total rows with cache: {total_rows_with_cache}")
    data_without_cache = pd.read_csv(file_path_withoutcache, sep='\t', header=0)
    total_rows_without_cache = len(data_without_cache)
    print(f"Total rows without cache: {total_rows_without_cache}")

    if total_rows_with_cache != total_rows_without_cache:
        print("Total rows are not the same!")
        exit(1)
    
    output_csv_writer = csv.writer(open(output_file_path, 'w'), delimiter='\t')
    output_csv_writer.writerow(['ID', 'Type', 'Count'])

    for i in range(total_rows_with_cache):
        output_csv_writer.writerow([data_with_cache.iloc[i, :]['ID'], "With", int(data_with_cache.iloc[i, :]['Count'])])
        output_csv_writer.writerow([data_without_cache.iloc[i, :]['ID'], "Without", int(data_without_cache.iloc[i, :]['Count'])])

file_path_witcache = sys.argv[1]
file_path_withoutcache = sys.argv[2]
output_file_path = sys.argv[3]

process_file(file_path_witcache, file_path_withoutcache, output_file_path)
import pandas as pd
import sys
import numpy as np

def process_file(file_path, target_count, output_file_path):
    data = pd.read_csv(file_path, sep='\t', header=0)
    total_rows = len(data)

    indices = np.linspace(0, total_rows - 1, target_count, dtype=int)
    
    selected_data = data.iloc[indices, :].copy()
    
    selected_data['ID'] = (np.arange(1, target_count + 1)) / target_count
    
    print(f"Total rows: {total_rows}")
    print(f"Interval: {indices[1] - indices[0] if len(indices) > 1 else total_rows}")
    print(f"Selected percentage setps: {target_count / total_rows:.4f}")
    print(f"Selected rows: {len(selected_data)}")
    # Check if the first row is the same
    if data.iloc[0, :]['Count'] != selected_data.iloc[0, :]['Count']:
        print("First row is not the same!")
        print(f"First row in original data: {data.iloc[0, :]['Count']}")
        print(f"First row in selected data: {selected_data.iloc[0, :]['Count']}")
    
    selected_data.to_csv(f"{output_file_path}", sep='\t', index=False)

file_path = sys.argv[1]
target_count = int(sys.argv[2])
output_file_path = sys.argv[3]
process_file(file_path, target_count, output_file_path)
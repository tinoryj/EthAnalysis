import pandas as pd
import sys
import csv
import numpy as np

def process_file(file_path, target_count, output_file_path):
    freq_list = []
    with open(file_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=';')
        for row in csv_reader:
            found = False
            for cell in row:
                if cell.startswith('Freq:'):
                    freq_value = cell.split(':', 1)[-1].strip()
                    freq_list.append(int(freq_value))
                    found = True
                    break
            if not found:
                freq_list.append(0)

    total_rows = len(freq_list)
    indices = np.linspace(0, total_rows - 1, target_count, dtype=int)
    
    selected_data = pd.DataFrame({
        'ID': np.arange(1, target_count + 1) / target_count,
        'Count': [freq_list[i] for i in indices]
    })
    
    selected_data = selected_data[['ID', 'Count']]
    selected_data.to_csv(output_file_path, sep='\t', index=False)
    print(f"Total rows: {total_rows} | Selected rows: {len(selected_data)}")

if __name__ == "__main__":
    process_file(sys.argv[1], int(sys.argv[2]), sys.argv[3])

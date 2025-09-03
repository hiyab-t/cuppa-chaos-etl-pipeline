import csv
import os

# List of input raw files
input_datas = [
    "data/chesterfield.csv",
    "data/leeds.csv",
    "data/uppingham.csv"
]

# Read full CSV rows
def extract_datas(datas):

    all_rows = []

    try:
        print('\nExtraction stage: starting...\n')
        for data in datas:
            with open(data, mode='r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)

                count = 0
                for row in reader:
                    if row:
                        all_rows.append(row)
                        count += 1

                print(f'Loaded {count} rows from {data}.\n')
        print(f'Loaded a total of {len(all_rows)} rows from {len(datas)} files.\n')

    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
    
    return all_rows

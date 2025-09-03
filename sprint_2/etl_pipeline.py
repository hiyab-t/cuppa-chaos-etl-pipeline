import os
import csv
import uuid
import re
from datetime import datetime
#from sql_utils import setup_db_connection




#extract_csv_data(input_datas)

def remove_pii_from_files(input_files):
    cleaned_data = {}

    for input_file in input_files:
        cleaned_rows = []

        with open(input_file, newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            for i, row in enumerate(reader):
                if i == 0:
                    # Write header for cleaned data
                    cleaned_rows.append([
                        "datetime",
                        "branch",
                        "product",
                        "total_price",
                        "payment_method"
                    ])
                    continue

                datetime_val, branch, _, product, total_price, payment_method, *_ = row

                cleaned_rows.append([
                    datetime_val,
                    branch,
                    product,
                    total_price,
                    payment_method
                ])

        cleaned_data[input_file] = cleaned_rows

    return cleaned_data





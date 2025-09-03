from datetime import datetime
from sql_utils import setup_db_connection
import re
import uuid
import csv

def generate_uuid():
    column_id = str(uuid.uuid4())

    return column_id

product_id = generate_uuid()

order_id = generate_uuid()

payment_id = generate_uuid()

branch_id = generate_uuid()

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

# standardization stage

# covert all dates to proper format to ensure accuracy
def convert_all_dates(list_of_dicts, date_cols,
                        current_format='%d/%m/%y',
                        expected_format='%y-%m-%d'):
    print(f'convert_all_dates...')

    for dict in list_of_dicts:
        for col in date_cols:
            try:
                str_to_date = datetime.strptime(dict[col], current_format)
                date_to_str = datetime.strftime(str_to_date, expected_format)
                dict[col] = date_to_str
            except ValueError as ex:
                print(f"convert_all_dates: Error parsing value '{dict[col]}' in column '{col}': {ex}")
                dict[col] = None

    return list_of_dicts

#check for float value and return error

def check_float_columns(list_of_dicts, float_cols):
    print(f'check_float_columns...')

    for dict in list_of_dicts:
        for col in float_cols:
            try:
                dict[col] = float(dict[col])
            except ValueError as ex:
                print(f'check_float_columns: Error parsing value "{dict[col]}" in column "{col}: {ex}')
            dict[col] = None

    return list_of_dicts

# checks if a string is fomartted properly then replaces the not allowed symbols

def check_and_format_str_columns_correctly(list_of_dicts, list_str_cols, cleaned_row):
    print(f'check_str_column and formatting them correctly')

    for dict in list_of_dicts:
        for col in list_str_cols:
            value = dict.get(col)
            if value is None:
                continue

        if not (re.fullmatch(r"^[A-Za-z0-9\s\-_\.,&'\"()/]+$"), str(value)):
            value = re.sub(r"[^A-Za-z0-9\s\-_\.,&'\"()/]", str(value), )
            value = value.strip().title()
    return list_of_dicts


# drops duplicate data

def drop_duplicate_ids_multiple_keys(list_of_dicts, id_keys):
    print(f'drop_duplicate_ids...')

    id_list = []

    list_with_uuids = []

    for dict in list_of_dicts:

        if dict[id_keys] not in id_list:
            id_list.append(dict[id_keys])
            list_with_uuids.append(dict)

    return list_with_uuids

# drops rows with null 

def drop_rows_with_null(list_of_dicts):
    print(f'drop_rows_with_null...')

    list_with_no_nulls = []

    for dict in list_of_dicts:
        if None not in dict.values() and '' not in dict.values():
            list_with_no_nulls.append(dict)

    return list_with_no_nulls

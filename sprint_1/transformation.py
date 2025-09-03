import csv
#import os
import re
import uuid
from collections import defaultdict


from datetime import datetime


datas = ['data/chesterfield.csv',
        'data/leeds.csv',
        'data/uppingham.csv']

#Generates uuid's
def generate_uuid():

    return str(uuid.uuid4())


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

def remove_sensitive_info(rows):

        cleaned_rows = []

            # Write header for cleaned data
        cleaned_rows.append([
            "datetime",
            "branch",
            "product",
            "total_price",
            "payment_method"
        ])

        for row in rows:
            if len(row) < 6:
                continue

            datetime_val, branch, _, product, total_price, payment_method, *_ = row

            cleaned_rows.append([
                datetime_val,
                branch,
                product,
                total_price,
                payment_method
            ])

        print('Removed PII from raw data.\n')

        return cleaned_rows

def parse_products(rows):

    print('Transformation stage: parsing products...')

    parsed_list = []

    for row_num, row in enumerate(rows, start=1):
        products = row[2].strip()
        if len(row) < 3 or not products:
            print(f'Skipping {row_num}. Not enough columns -> {row}.')
            continue

        if row_num == 1:
            print('Skipping header...\n')
            continue

        product_and_price = [p.strip() for p in products.split(',')]

        for p in product_and_price:
                parts = [prod for prod in p.split(' - ')]

                if len(parts) == 3:
                    size_type, flavour, price = parts
                elif len(parts) == 2:
                    size_type, price = parts
                    flavour = None
                else:
                    raise ValueError(f'Unexpected format: {p}')

                size_type = size_type.strip().title()

                match1 = re.search('Regular', size_type)
                match2 = re.search ('Large', size_type)

                if match1 or match2:
                    size_parts = size_type.split(' ', 1)
                    size = size_parts[0]
                    name = size_parts[1] if len(size_parts) > 1 else None
                else:
                    size = None
                    name = size_type

                parsed = {
                "size": size,
                "name": name,
                "flavour": flavour,
                "price": price
                }

                parsed_list.append(parsed)

    return parsed_list

# check if string columns are properly formatted and reformat them if not

def check_and_format_str_columns_correctly(parsed_list, cols_str_list):

    for dict in parsed_list:
        for col in cols_str_list:
            value = dict.get(col)

            if value is None:
                continue

            if not (re.fullmatch(r"^[A-Za-z0-9\s'()\./]+$", str(value))):
                value = re.sub(r"[^A-Za-z0-9\s'()\./]+", "",str(value), flags=re.UNICODE)
                value = value.strip().title()
                value = dict[col]
    
    return parsed_list

#check for float value and return error

def check_float_columns(list_of_dicts, float_cols):
    print('Check float columns...')

    for dict in list_of_dicts:
        for col in float_cols:
            try:
                dict[col] = float(dict[col])
            except ValueError as ex:
                print(f'check_float_columns: Error parsing value "{dict[col]}" in column "{col}: {ex}')
                dict[col] = None

    return list_of_dicts

def check_int_columns(list_of_dicts, int_cols):
    print('Check float columns...')

    for dict in list_of_dicts:
        for col in int_cols:
            try:
                dict[col] = int(dict[col])
            except ValueError as ex:
                print(f'check_float_columns: Error parsing value "{dict[col]}" in column "{col}: {ex}')
                dict[col] = None

    return list_of_dicts

# covert all dates to proper format to ensure accuracy

def convert_all_dates(list_of_dicts, date_cols,
                        current_format='%d/%m/%Y %H:%M',
                        expected_format='%Y-%m-%d %H:%M'):
    
    print('Convert all dates...')

    for dict in list_of_dicts:
        for col in date_cols:
            try:
                str_to_date = datetime.strptime(dict[col], current_format)
                date_to_str = datetime.strftime(str_to_date, expected_format)
                dict[col] = date_to_str
            except ValueError as ex:
                print(f"Convert all dates: Error parsing value '{dict[col]}' in column '{col}': {ex}")
                dict[col] = None

    return list_of_dicts

def drop_duplicate_product_values(list_of_dict):

    seen_product = []

    unique_rows = []

    for dict in list_of_dict:
        keys = (dict['size'] if dict["size"] else None,
                dict['name'],
                dict['flavour'] if dict["flavour"] else None, 
                dict['price'])
        
        if keys not in seen_product:
            seen_product.append(keys)
            unique_rows.append(dict)
        else:
            continue

    return unique_rows

def normalize_product_table(unique_rows):

    product_list = []

    for row in unique_rows:
        keys = {"product_id": generate_uuid(),
                "size": row['size'] if row["size"] else None,
                "name": row['name'],
                "flavour": row['flavour'] if row["flavour"] else None, 
                "price": row['price']}
        
        product_list.append(keys)

    return product_list

def drop_duplicate_branches(list_of_dict):
    
    seen_product = []

    unique_rows = []

    for dict in list_of_dict:
        if (dict['branch_name']) not in seen_product:
            seen_product.append(dict['branch_name'])
            unique_rows.append(dict)
        else:
            continue

    return unique_rows

def normalize_branches(cleaned_rows):
    branches = []

    for row_num, row in enumerate(cleaned_rows[1:], start=2):  # Skip header
        if len(row) < 5:
            print(f"Skipping malformed row {row_num}: {row}")
            continue

        _, branch_name, _, _, _ = row

        branches.append({
            "branch_id": generate_uuid(),
            "branch_name": branch_name
        })

    return branches

#Creates a normalised orders table
def normalise_orders(removed_pii, products_table, branch_table):
    print("Normalising orders...")

    normalised_orders = []

    branch_lookup = {b['branch_name']: b['branch_id'] for b in branch_table}
    product_lookup = {(p['size'], p['name'], p['flavour'], p['price']): p['product_id'] for p in products_table}

    parsed_products = parse_products(removed_pii)
    parsed_products = check_and_format_str_columns_correctly(parsed_products, ['size', 'name', 'flavour'])

    parsed_index = 0

    for row_num, row in enumerate(removed_pii[1:], start=2):
        if len(row) < 5:
            continue

        datetime_val, branch_name, products_str, total_price, payment_method = row
        branch_id = branch_lookup.get(branch_name)
        if not branch_id:
            continue

        order_id = generate_uuid()

        # Count products in this order
        quantity_counter = defaultdict(int)
        num_products = len(products_str.split(','))

        for _ in range(num_products):
            parsed_product = parsed_products[parsed_index]
            parsed_index += 1
            key = (parsed_product['size'], parsed_product['name'], parsed_product['flavour'], parsed_product['price'])
            product_id = product_lookup.get(key)
            if product_id:
                quantity_counter[product_id] += 1

        # Append to normalised orders
        for product_id, quantity in quantity_counter.items():
            normalised_orders.append({
                "order_id": order_id,
                "branch_id": branch_id,
                "product_id": product_id,
                "quantity": quantity,
                "order_time": datetime_val,
                "total_price": total_price,
                "payment_method": payment_method
            })

    print(f"Created {len(normalised_orders)} normalised order rows.\n")
    return normalised_orders


raw_data = extract_datas(datas)
removed_pii = remove_sensitive_info(raw_data)
parsed = parse_products(removed_pii)
correctly_formatted_products = check_and_format_str_columns_correctly(parsed, cols_str_list=['size', 'name', 'flavour'])
validated_float_cols = check_float_columns(correctly_formatted_products, float_cols=['price'])
unique_rows_products = drop_duplicate_product_values(correctly_formatted_products)
uncleaned_branch_table = normalize_branches(removed_pii)
clean_products_table = normalize_product_table(unique_rows_products)
clean_branch_table = drop_duplicate_branches(uncleaned_branch_table)

clean_orders_table = normalise_orders(removed_pii, clean_products_table, clean_branch_table)



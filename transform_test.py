import csv
import re
import uuid
import os
from datetime import datetime
from collections import defaultdict


datas = [
    'sprint_1/data/chesterfield.csv',
    'sprint_1/data/leeds.csv',
    'sprint_1/data/uppingham.csv'
]

#Generates uuid's
def generate_uuid():

    return str(uuid.uuid4())

def extract_datas(datas):
    all_rows = []
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
    return all_rows

def remove_pii_from_files(rows):
    cleaned_rows = []
    # Write header for cleaned data
    cleaned_rows.append(["datetime", "branch", "products", "total_price", "payment_method"])
    
    for row in rows:
        datetime_val, branch, _, product, total_price, payment_method, *_ = row
        cleaned_rows.append([datetime_val, branch, product, total_price, payment_method])
    
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
            parts = [prod.strip() for prod in p.split(' - ')]
            if len(parts) == 3:
                size_type, flavour, price = parts
            elif len(parts) == 2:
                size_type, price = parts
                flavour = None
            else:
                print(f"Skipping unexpected format: {p}")
                continue

            size_type = size_type.title()
            match1 = re.search('Regular', size_type)
            match2 = re.search('Large', size_type)

            if match1 or match2:
                size_parts = size_type.split(' ', 1)
                size = size_parts[0]
                name = size_parts[1] if len(size_parts) > 1 else None
            else:
                size = None
                name = size_type

            parsed_list.append({
                "size": size,
                "name": name,
                "flavour": flavour,
                "price": float(price)
            })

    return parsed_list


def check_and_format_str_columns_correctly(parsed_list, cols_str_list):
    for dict_ in parsed_list:
        for col in cols_str_list:
            value = dict_.get(col)
            if value is None:
                continue
            value = re.sub(r"[^A-Za-z0-9\s'()\./]+", "", str(value)).strip().title()
            dict_[col] = value
    return parsed_list

#check for float value and return error

def normalize_branches(cleaned_rows):
    branches = []
    for row_num, row in enumerate(cleaned_rows[1:], start=2):
        if len(row) < 5:
            print(f"Skipping malformed row {row_num}: {row}")
            continue
        _, branch_name, _, _, _ = row
        branches.append({"branch_id": generate_uuid(), "branch_name": branch_name})
    return branches

def drop_duplicate_branches(branches):
    seen = set()
    unique_rows = []
    for b in branches:
        if b['branch_name'] not in seen:
            seen.add(b['branch_name'])
            unique_rows.append(b)
    print(f"Unique branches: {len(unique_rows)}")
    return unique_rows


def drop_duplicate_product_values(parsed_list):
    seen = set()
    unique_rows = []
    for p in parsed_list:
        key = (p['size'], p['name'], p['flavour'], p['price'])
        if key not in seen:
            seen.add(key)
            unique_rows.append(p)
    print(f"Unique products: {len(unique_rows)}")
    return unique_rows

def normalize_product_table(unique_rows):
    product_list = []
    for row in unique_rows:
        product_list.append({
            "product_id": generate_uuid(),
            "size": row['size'],
            "name": row['name'],
            "flavour": row['flavour'],
            "price": row['price']
        })
    return product_list


def normalise_orders(removed_pii, products_table, branch_table, payments_table):
    print("Normalising orders...")

    normalised_orders = []

    branch_lookup = {b['branch_name']: b['branch_id'] for b in branch_table}
    product_lookup = {(p['size'], p['name'], p['flavour'], p['price']): p['product_id'] for p in products_table}
    payment_lookup = {(pay['total_price'], pay['datetime'], pay['payment_method']): pay['payment_id'] for pay in payments_table}

    parsed_products = parse_products(removed_pii)
    parsed_products = check_and_format_str_columns_correctly(parsed_products, ['size', 'name', 'flavour'])

    parsed_index = 0

    for row_num, row in enumerate(removed_pii[1:], start=2):
        if len(row) < 5:
            continue

        datetime_val, branch_name, products_str, total_price, payment_method = row
        branch_id = branch_lookup.get(branch_name)
        payment_id = payment_lookup.get(datetime_val, total_price, payment_method)
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
                "payment_id": payment_id,
            })

    print(f"Created {len(normalised_orders)} normalised order rows.\n")
    return normalised_orders



raw_data = extract_datas(datas)
removed_pii = remove_pii_from_files(raw_data)
parsed_products = parse_products(removed_pii)
parsed_products = check_and_format_str_columns_correctly(parsed_products, ['size', 'name', 'flavour'])
unique_products = drop_duplicate_product_values(parsed_products)
products_table = normalize_product_table(unique_products)

branch_table = normalize_branches(removed_pii)
branch_table = drop_duplicate_branches(branch_table)

normalised_orders = normalise_orders(removed_pii, products_table, branch_table)

# Show results
print("First 10 normalised orders:")
for row in normalised_orders[:20]:
    print(row)
print(f"\nTotal normalised order rows: {len(normalised_orders)}")

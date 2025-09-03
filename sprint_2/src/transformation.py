import logging
import uuid
from datetime import datetime
from collections import defaultdict
import re


LOGGER = logging.getLogger()

LOGGER.setLevel(logging.INFO)

# Generate UUIDs
def generate_uuid():
    return str(uuid.uuid4())

# Remove PII, keep only relevant columns
def remove_sensitive_info(rows):
    cleaned_rows = [["datetime", "branch", "product", "total_price", "payment_method"]]
    for row in rows:
        if len(row) < 6:
            continue
        datetime_val, branch, _, product, total_price, payment_method, *_ = row
        cleaned_rows.append([datetime_val, branch, product, total_price, payment_method])
    return cleaned_rows

# Parse products into structured dicts
def parse_products(rows):
    parsed_list = []
    for row_num, row in enumerate(rows[1:], start=2):
        products_str = row[2].strip()
        if not products_str:
            continue
        product_items = [p.strip() for p in products_str.split(',')]
        for p in product_items:
            parts = [x.strip() for x in p.split(' - ')]
            if len(parts) == 3:
                type_size, flavour, price = parts
            elif len(parts) == 2:
                type_size, price = parts
                flavour = None
            else:
                continue

            match = re.match(r"(Regular|Large)\s+(.*)", type_size.title())
            if match:
                size, type_name = match.groups()
            else:
                size = None
                type_name = type_size.title()

            parsed_list.append({
                "size": size,
                "name": type_name,
                "flavour": flavour.title() if flavour else None,
                "price": float(price)
            })
    return parsed_list

# Drop duplicates and reuse UUIDs from DB
def get_existing_products(cursor):
    cursor.execute("SELECT product_id, name, size, flavour FROM products")
    return { (row[1], row[2], row[3]): row[0] for row in cursor.fetchall() }

def drop_duplicate_product_values(parsed_list, cursor):
    seen = get_existing_products(cursor)
    unique_rows = []
    for row in parsed_list:
        key = (row['name'], row['size'], row['flavour'])
        if key in seen:
            row['product_id'] = seen[key]
        else:
            row['product_id'] = generate_uuid()
            seen[key] = row['product_id']
            unique_rows.append(row)
    return unique_rows

# Normalize branches, reuse UUIDs from DB
def get_existing_branches(cursor):
    cursor.execute("SELECT branch_id, branch_name FROM branches")
    return { row[1]: row[0] for row in cursor.fetchall() }

def normalize_branches(rows, cursor):
    branch_list = []
    seen = get_existing_branches(cursor)
    for row in rows[1:]:
        branch_name = row[1]
        if branch_name not in seen:
            branch_id = generate_uuid()
            seen[branch_name] = branch_id
            branch_list.append({"branch_id": branch_id, "branch_name": branch_name})
    return branch_list

# Normalize orders
def normalize_orders(rows, products_table, branches_table):
    normalised_orders = []

    product_lookup = {(p['name'], p['size'], p['flavour']): p['product_id'] for p in products_table}
    branch_lookup = {b['branch_name']: b['branch_id'] for b in branches_table}

    for row in rows[1:]:
        datetime_str, branch_name, products_str, total_price, payment_method = row
        if not datetime_str or not products_str:
            continue

        try:
            order_date = datetime.strptime(datetime_str, "%d/%m/%Y %H:%M")
        except ValueError:
            continue

        branch_id = branch_lookup.get(branch_name)
        if not branch_id:
            continue

        product_items = [p.strip() for p in products_str.split(',')]
        quantity_counter = defaultdict(int)
        product_prices = {}

        for p in product_items:
            parts = [x.strip() for x in p.split(' - ')]
            if len(parts) == 3:
                type_size, flavour, price = parts
            elif len(parts) == 2:
                type_size, price = parts
                flavour = None
            else:
                continue

            match = re.match(r"(Regular|Large)\s+(.*)", type_size.title())
            if match:
                size, name = match.groups()
            else:
                size = None
                name = type_size.title()

            key = (name, size, flavour.title() if flavour else None)
            product_id = product_lookup.get(key)
            if not product_id:
                continue

            quantity_counter[product_id] += 1
            product_prices[product_id] = (price)

        order_id = generate_uuid()
        for product_id, qty in quantity_counter.items():
            normalised_orders.append({
                "order_id": order_id,
                "branch_id": branch_id,
                "product_id": product_id,
                "quantity": qty,
                "order_date": order_date,
                "total_price": product_prices[product_id] * qty,
                "payment_method": payment_method
            })

    return normalised_orders

def transformation(data, cursor):

    LOGGER.info('Transformation stage: parsing products...')

    LOGGER.info(f'Remove sensitive information: processing rows={len(data)}')

    removed_pii = remove_sensitive_info(data)

    LOGGER.info("Removed all sensitive information.")

    LOGGER.info("Parsing: starting...")

    parsed = parse_products(removed_pii)

    LOGGER.info('Parsing products done.')

    LOGGER.info('Dropping duplicate product values...')

    LOGGER.info('Normalizing products...')

    clean_products_table = drop_duplicate_product_values(parsed, cursor)

    LOGGER.info('Product table normalised.')

    LOGGER.info('Dropping duplicate branches...')
    
    LOGGER.info('Normalising branches...')

    clean_branch_table = normalize_branches(removed_pii, cursor)

    LOGGER.info('Branches normalised.')

    LOGGER.info('Orders normalised')

    clean_orders_table = normalize_orders(removed_pii, clean_products_table, clean_branch_table)


    return {
    "products": clean_products_table,
    "branches": clean_branch_table,
    "orders": clean_orders_table
}



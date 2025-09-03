import csv
import os
import io
import logging
import re

LOGGER = logging.getLogger()

LOGGER.setLevel(logging.INFO)

def extract(body_text):
    LOGGER.info('Extract: starting...')

    reader = csv.reader(io.StringIO(body_text), delimiter=',')

    data = [row for row in reader if row]

    LOGGER.info(f'Extract: done: rows{len(data)}')

    return data

def remove_sensitive_info(data):

    LOGGER.info(f'Remove sensitive information: processing rows={len(data)}')

    cleaned_data = []

        # Write header for cleaned data
    cleaned_data.append([
            "datetime",
            "branch",
            "product",
            "total_price",
            "payment_method"
        ])
    
    for row in data[1:]:
        if len(row) < 6:
            continue

        datetime_val, branch, _, product, total_price, payment_method, *_ = row

        cleaned_data.append([
                datetime_val,
                branch,
                product,
                total_price,
                payment_method
            ])
        
    LOGGER.info("Removed all sensitive information.")

    return cleaned_data

def parse_products(data):

    LOGGER.info('Transformation stage: parsing products...')

    parsed_list = []

    for row_num, row in enumerate(data, start=1):
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

        LOGGER.info('Parsing products done.')

    return parsed_list




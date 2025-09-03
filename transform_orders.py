import csv
import os
import uuid
from extract_products import parse_product  # Reuse the existing parser


# Export the orders data to a CSV file
# reads from cleaned csv files and returns all rows as a list of dictionaries 
def extract_orders_csv(file_path):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [row for row in reader if row]



# Transform orders into normalized format
# transform rows of cleaned orders into normalised format:
# one row per product, with UUID order_id and product_id 
def transform_orders(order_rows, product_lookup, branch_id):
    normalised_orders = []
    
    for row in order_rows:
        product_items = row[2].split(', ')
        order_id = str(uuid.uuid4())
        quantity_counter = {} # counts how mnay times each product was ordered 
        
        for item in product_items:
            parsed = parse_product(item.strip())
            key = (parsed['size'], parsed['type'], parsed['flavour'], parsed['price'])
        
            if key in product_lookup: #checking products before duplicates were dropped against product table
                product_id = product_lookup[key]
                if product_id in quantity_counter:
                    quantity_counter[product_id] += 1
                else:
                    quantity_counter[product_id] = 1 
            
            else:
                print(f"Product not found in lookup: {key}")
        
        # add one row per unique product_id with quanity 
        for product_id, qty in quantity_counter.items():
                normalised_orders.append({
                    'order_id': order_id,
                    'branch_id': branch_id,
                    'product_id': product_id,
                    'quantity': qty
                })

    return normalised_orders

# export the normalized orders to CSV - wiritng final normailsed order data 
def export_orders_csv(order_data, filename):
    fieldnames = ['order_id', 'branch_id', 'product_id', 'quantity']
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(order_data)

# --- MAIN EXECUTION ---
# Input cleaned CSV files for each branch
if __name__ == "__main__":
    input_files = {
        'output/chesterfield_clean.csv': 'chesterfield',
        'output/leeds_clean.csv': 'leeds',
        'output/uppingham_clean.csv': 'uppingham'
    }

    # Product lookup CSVs
    product_files = {
        'chesterfield': 'products_output/chesterfield_products.csv',
        'leeds': 'products_output/leeds_products.csv',
        'uppingham': 'products_output/uppingham_products.csv'
    }
    
    # Branch ID lookup table (add the real uuids - this is just a sample )
    branch_lookup = {
        'chesterfield': 'c1',
        'leeds': 'l2',
        'uppingham': 'u3'
    }
    
    # Create output folder if it doesn't exist
    output_dir = "orders_output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Loop through all cleaned branch files
    for file_path, branch_name in input_files.items():
        branch_id = branch_lookup[branch_name]
        
        # Step 1: Load cleaned orders data
        order_rows = extract_orders_csv(file_path)
        print(order_rows)
        
        # Step 2: Load product lookup
        product_lookup = {}
        with open(product_files[branch_name], newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                key = (
                    row['size'],
                    row['type'],
                    row['flavour'] if row['flavour'] else None,
                    float(row['price'])
                )
                
                product_lookup[key] = row['product_id']
        
        # Step 3: Transform orders into normalized format
        orders = transform_orders(order_rows, product_lookup, branch_id)
        
        # Step 4: Export to output CSV
        output_file = os.path.join(output_dir, f"{branch_name}_orders.csv")
        export_orders_csv(orders, output_file)
        print(f"Saved {len(orders)} orders to {output_file}")



        def normalise_orders(removed_pii, products_table, branch_table):
    
    print("Normalising orders...")

    normalised_orders = []

    branch_lookup = {b['branch_name']: b['branch_id'] for b in branch_table}
    product_lookup = {(p['size'], p['name'], p['flavour'], p['price']): p['product_id'] for p in products_table}
    payment_lookup = {p['payment_id']: }

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
        payment_id = generate_uuid()

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
                "datetime": datetime_val,
                "payment_id": payment_id,
                "payment_method": payment_method
            })

    print(f"Created {len(normalised_orders)} normalised order rows.\n")
    return normalised_orders
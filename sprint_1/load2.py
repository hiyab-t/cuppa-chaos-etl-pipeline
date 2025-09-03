from transformation2 import clean_products_table, clean_branch_table, clean_orders_table
from sql_utils import setup_db_connection

def insert_products(products, cursor, connection):
    sql = """
    INSERT INTO products (product_id, type, size, flavour, price)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (product_id) DO NOTHING
    """
    for p in products:
        cursor.execute(sql, (p['product_id'], p['name'], p['size'], p['flavour'], p['price']))
    connection.commit()

def insert_branches(branches, cursor, connection):
    sql = """
    INSERT INTO branches (branch_id, branch_name)
    VALUES (%s, %s)
    ON CONFLICT (branch_id) DO NOTHING
    """
    for b in branches:
        cursor.execute(sql, (b['branch_id'], b['branch_name']))
    connection.commit()

def insert_orders(orders, cursor, connection):
    sql = """
    INSERT INTO orders (order_id, branch_id, product_id, quantity, order_date, total_price, payment_method)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (order_id, product_id) DO UPDATE
    SET quantity = orders.quantity + EXCLUDED.quantity
    """
    for o in orders:
        cursor.execute(sql, (
            o['order_id'],
            o['branch_id'],
            o['product_id'],
            o['quantity'],
            o['order_date'],
            o['total_price'],
            o['payment_method']
        ))
    connection.commit()

def load_local():
    conn, cursor = setup_db_connection()
    insert_products(clean_products_table, cursor, conn)
    insert_branches(clean_branch_table, cursor, conn)
    insert_orders(clean_orders_table, cursor, conn)
    print("Loaded transformed data into local database")

    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_local()

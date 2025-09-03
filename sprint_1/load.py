from transformation import *
from sql_utils import setup_db_connection
from transformation import clean_products_table, clean_branch_table, clean_orders_table



def insert_products(products, cursor, connection):
    sql = '''
        INSERT INTO products (product_id, type, size, flavor, price)
        VALUES (%s, %s, %s, %s, %s)
    '''
    for col in products:
        data_values = (
            col["product_id"],
            col["type"],
            col["size"], 
            col["flavour"], 
            col["price"])
        
    cursor.execute(sql, data_values)
    connection.commit()
       
    cursor.execute(sql, data_values)
    connection.commit()




def insert_branches(branches, cursor, connection):
   sql = '''
       INSERT INTO branches (branch_id, branch_name)
       VALUES (%s, %s)
   '''
   for col in branches:
       data_values = (
           col["branch_id"], 
           col["branch_name"])
       
       cursor.execute(sql, data_values)
   connection.commit()


def insert_orders(orders, cursor, connection):

    sql = '''
        INSERT INTO orders (order_id, branch_id, product_id, quanntity, order_time, total_price, payment_method)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''

    for col in orders:
        date_values = ( col["order_id"],
                        col["Branch_id"],
                        col["product_id"],
                        col["quanntity"],
                        col["order_time"],
                        col["total_price"],
                        col["payment_method"])
        
    cursor.execute(sql, date_values)
    connection.commit()


def load_local():
    cursor, connection = setup_db_connection()
    if not cursor or not connection:
        print("Database connection failed!")
        return
    
    insert_products(clean_products_table, cursor, connection)
    insert_branches(clean_branch_table, cursor, connection)
    insert_orders(clean_orders_table, cursor, connection)

    print("Loaded transformed data into local datatbase")



if __name__ == "__main__":
    load_local()
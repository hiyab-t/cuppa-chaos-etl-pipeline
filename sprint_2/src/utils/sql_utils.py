import uuid
import logging

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def create_db_tables(connection, cursor):
    LOGGER.info('create_db_tables: started')
    try:

        LOGGER.info('create_db_tables: creating products, branches and orders table')

        create_products_table ='''
                        CREATE TABLE IF NOT EXISTS products (
                        product_id VARCHAR(36) PRIMARY KEY,
                        name VARCHAR(50) NOT NULL,
                        size VARCHAR(50),
                        flavour VARCHAR(50),
                        price DECIMAL(10, 2) NOT NULL
                        );'''
        
        create_branch_table = '''
                            CREATE TABLE IF NOT EXISTS branches (
                            branch_id VARCHAR(36) PRIMARY KEY,
                            branch_name VARCHAR(20) NOT NULL
                            );'''
        
        create_orders_table = '''
                            CREATE TABLE IF NOT EXISTS orders (
                            order_id VARCHAR(36) PRIMARY KEY,
                            branch_id VARCHAR(36) NOT NULL,
                            product_id VARCHAR(36) NOT NULL,
                            quantity INT NOT NULL,
                            order_time TIMESTAMP NOT NULL,
                            total_price DECIMAL(10, 2) NOT NULL,
                            payment_method VARCHAR(10) NOT NULL
                            );'''        
        cursor.execute(create_products_table)
        cursor.execute(create_branch_table)
        cursor.execute(create_orders_table)

        LOGGER.info('create_db_tables: committing')
        connection.commit()

        LOGGER.info('create_db_tables: done')
    except Exception as ex:
        LOGGER.info(f'create_db_tables: failed to run sql: {ex}')
        raise
    

def save_data_in_db(connection, cursor, table: str, data: list, columns: list, commit_every: int = 1000):
    if not data:
        LOGGER.info('save_data_in_db: no rows to insert for table=%s', table)
        return
    
    col_list_sql = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    pk_map = {"branches": "branch_id", "products": "product_id", "orders": "order_id"}
    pk_col = pk_map[table]

    sql = f"""
        INSERT INTO {table} ({col_list_sql})
        VALUES ({placeholders})
        ON CONFLICT ({pk_col}) DO NOTHING
    """

    LOGGER.info("save_data_in_db: start tables=%s, rows=%d", table, len(data))
    try:
        count = 0
        for row in data:
            values = [row[col] for col in columns]
            cursor.execute(sql, values)
            count += 1
            if count % commit_every == 0:
                connection.commit()
                LOGGER.info("save_data_in_db: committed batch of %d to %s", commit_every, table)

        connection.commit()
        LOGGER.info("save_data_in_db: done table=%s, total_rows=%d", table, count)

    except Exception as ex:
        connection.rollback()
        LOGGER.error("save_data_in_db: errorrable=%s, ex=%s", table, ex)
        raise


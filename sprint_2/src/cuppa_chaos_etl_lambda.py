import logging
import os
import json
from utils import s3_utils, db_utils, sql_utils
import extract, transformation



LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

SSM_ENV_VAR_NAME = 'SSM_PARAMETER_NAME'

def lambda_handler(event, context):

    LOGGER.info('Lambda handler: Starting')
    file_path = 'NOT_SET'

    try:

        bucket_name, file_path = s3_utils.get_file_info(event)

        csv_text = s3_utils.load_file(bucket_name, file_path)

        data = extract.extract(csv_text)

        ssm_param_name = os.environ.get(SSM_ENV_VAR_NAME, 'NOT_SET')
        LOGGER.info(f'lambda_handler: ssm_param_name={ssm_param_name} from ssm_env_var_name={SSM_ENV_VAR_NAME}')
        redshift_details = db_utils.get_ssm_param(ssm_param_name)
        conn, cur = db_utils.open_sql_database_connection_and_cursor(redshift_details)

        sql_utils.create_db_tables(conn, cur)

        transformed_data = transformation.transformation(data, cur)

        LOGGER.info(f'lambda_handler: done, file={file_path}')

        LOGGER.warning(f'lambda_handler: transformed_data={transformed_data}')

        
        sql_utils.save_data_in_db(conn, cur,
                                    table="products",
                                    data=transformed_data["products"],
                                    columns=["product_id", "name", "size", "flavour", "price"])
        sql_utils.save_data_in_db(conn, cur,
                                    table="branches",
                                    data=transformed_data["branches"],
                                    columns=["branch_id", "branch_name"])
        sql_utils.save_data_in_db(conn, cur,
                                    table="orders",
                                    data=transformed_data["orders"],
                                    columns=["order_id", "branch_id", "product_id", "quantity", "order_date", "total_price", "payment_method"])

        cur.close()
        conn.close()

        LOGGER.info(f'lambda_handler: done, file={file_path}')

    except Exception as err:
        LOGGER.error(f"lambda_handler: failure: {err=}, {type(err)=}, file={file_path}")
        raise err
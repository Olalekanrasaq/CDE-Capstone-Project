import boto3
import psycopg2
import pandas as pd
import os
from include.extraction.get_s3_files import aws_session

# create boto session and ssm client
session = aws_session()
ssm_client = session.client('ssm')

# database connection credentials
db_host = ssm_client.get_parameter(Name='/coretelecomms/database/db_host')
db_name = ssm_client.get_parameter(Name='/coretelecomms/database/db_name')
db_password = ssm_client.get_parameter(Name='/coretelecomms/database/db_password')
db_username = ssm_client.get_parameter(Name='/coretelecomms/database/db_username')
db_port = ssm_client.get_parameter(Name='/coretelecomms/database/db_port')
db_schema = ssm_client.get_parameter(Name='/coretelecomms/database/table_schema_name')

# output file path
output_file_path = '/opt/airflow/dags/include/data_sources/website_forms'

def copy_postgres_table():
    '''
    A function that copy data from postgres database table
    '''
    # create a sub-directory where the output data will be written to if not exist
    os.makedirs(output_file_path, exist_ok=True)

    # connect to the database
    conn = psycopg2.connect(
        host=db_host['Parameter']['Value'],
        dbname=db_name['Parameter']['Value'],
        user=db_username['Parameter']['Value'],
        password=db_password['Parameter']['Value'],
        port=db_port['Parameter']['Value']
    )

    # create a cursor object and retrieve all tables in the target schema
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        AND table_schema = %s;
    """, (db_schema['Parameter']['Value'],))

    tables = [table[0] for table in cur.fetchall()]

    for table in tables:
        # create a file path to store the table data
        table_file_path = os.path.join(output_file_path, f'{table}.parquet')
        # check if the file path already exist
        if os.path.exists(table_file_path):
            continue

        df = pd.read_sql(f"SELECT * FROM {db_schema['Parameter']['Value']}.{table}", conn)

        # load the dataframe to parquet file
        df.to_parquet(table_file_path)
        print(f'Table {table} extracted into parquet successfully!!')
    print("Copying database tables operation completed.")
    conn.close()



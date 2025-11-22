import boto3
from include.get_s3_files import aws_session
import psycopg2
import pandas as pd

# create boto session and ssm client
session = aws_session()
ssm_client = session.client('ssm')

db_host = ssm_client.get_parameter(Name='/coretelecomms/database/db_host')
db_name = ssm_client.get_parameter(Name='/coretelecomms/database/db_name')
db_password = ssm_client.get_parameter(Name='/coretelecomms/database/db_password')
db_username = ssm_client.get_parameter(Name='/coretelecomms/database/db_username')
db_port = ssm_client.get_parameter(Name='/coretelecomms/database/db_port')
db_schema = ssm_client.get_parameter(Name='/coretelecomms/database/table_schema_name')

def copy_postgres_table():
    '''
    A function that copy data from postgres database table
    '''
    conn = psycopg2.connect(
        host=db_host['Parameter']['Value'],
        dbname=db_name['Parameter']['Value'],
        user=db_username['Parameter']['Value'],
        password=db_password['Parameter']['Value'],
        port=db_port['Parameter']['Value']
    )

    df = pd.read_sql("SELECT * FROM customer_complaints.Web_form_request_2025_11_20 LIMIT 10", conn)
    conn.close()
    print('Data has been read to dataframe successfully')
    print(df)



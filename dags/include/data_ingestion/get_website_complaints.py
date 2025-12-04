import boto3
import psycopg2
import pandas as pd
import io
from datetime import datetime
from include.data_ingestion.get_s3_files import get_boto3_client

# create boto session and ssm client
ssm_client = get_boto3_client('aws_source', 'eu-north-1', 'ssm')

# database connection credentials
db_host = ssm_client.get_parameter(Name='/coretelecomms/database/db_host')
db_name = ssm_client.get_parameter(Name='/coretelecomms/database/db_name')
db_password = ssm_client.get_parameter(Name='/coretelecomms/database/db_password')
db_username = ssm_client.get_parameter(Name='/coretelecomms/database/db_username')
db_port = ssm_client.get_parameter(Name='/coretelecomms/database/db_port')
db_schema = ssm_client.get_parameter(Name='/coretelecomms/database/table_schema_name')

# s3 bucket parameter
dest_bucket = 'cde-capstone-olalekan'
key_prefix = 'website_forms'

def copy_postgres_table():
    '''
    A function that copy data from postgres database table
    '''

    # list all the existing keys in website_forms s3 bucket subfolder
    s3_dest = get_boto3_client('aws_dest', 'us-east-1', 's3')
    objs = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix=key_prefix)
    objs_list = [obj['Key'] for obj in objs.get('Contents', [])]

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

    # get the name of the tables from the cursor tuple output
    tables = [table[0] for table in cur.fetchall()]

    for table in tables:

        # create the key to be ued to save the file in s3
        dest_key = f"{key_prefix}/{table}.parquet"
        
        # check if the key (filepath) already exists in the s3 bucket
        if dest_key in objs_list:
            print(f"{dest_key} already exists in {dest_bucket}...")
            continue
        
        # read the content of the database into a pandas dataframe
        df = pd.read_sql(f"SELECT * FROM {db_schema['Parameter']['Value']}.{table}", conn)
        df = df.set_index(df.columns[0])

        # inlcude the data load time
        df['ingested_at'] = datetime.now()

        # Convert to Parquet (in memory)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=False)

        # Upload Parquet to destination S3
        s3_dest.put_object(
            Bucket=dest_bucket,
            Key=dest_key,
            Body=parquet_buffer.getvalue(),
            Metadata={
                "load_time": datetime.utcnow().isoformat(),
                "source_file": "Postgres Transactional Database",
                "record_count": str(len(df)),
                "no_of_columns": str(len(df.columns))
            }
        )

        print(f'Table {table} written to s3 successfully!!')
    print("Copying database tables operation completed.")
    conn.close()



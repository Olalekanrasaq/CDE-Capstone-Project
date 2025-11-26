import boto3
import pandas as pd
import io
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook

# s3 bucket parameters
source_bucket = 'core-telecoms-data-lake'
dest_bucket = 'cde-capstone-olalekan'
s3_keys = ['call logs', 'customers', 'social_medias']

def get_boto3_client(conn_id, region, service):
    '''
    A wrapper function around boto3 to interact with AWS
    '''
    hook = AwsGenericHook(aws_conn_id=conn_id, region_name=region, client_type=service)
    return hook.get_client_type()

def transfer_s3_files():
    '''
    A function that transfer s3 files between buckets in two different s3 accounts
    '''

    # create connection to source and destination s3 buckets
    s3_source = get_boto3_client('aws_source', 'eu-north-1', 's3')
    s3_dest = get_boto3_client('aws_dest', 'us-east-1', 's3')

    for s3_key in s3_keys:

        # list all the objects in both source and destination bucket keys
        objs_source = s3_source.list_objects_v2(Bucket=source_bucket, Prefix=s3_key)
        objs_dest = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix=s3_key)

        source_keys = [obj['Key'] for obj in objs_source.get('Contents', [])]
        dest_keys = [obj['Key'] for obj in objs_dest.get('Contents', [])]


        for key in source_keys:
            # check if the key is a directory and skip
            if key.endswith('/'):
                print(f"{key} is not a file")
                continue
            
            # check if object is a csv or json file 
            # and create a destination object file path
            if key.endswith('.csv'):
                dest_key = f'{key[:-3]}parquet'
            elif key.endswith('.json'):
                dest_key = f'{key[:-4]}parquet'
            else:
                print(f"{key} not a target file to extract!")
                continue
            
            # check if the object already exists in destination bucket key... if found, skip
            if dest_key in dest_keys:
                print(f'{dest_key} already exist in {dest_bucket}')
                continue 
            
            # get the source s3 object and read to pandas
            file = s3_source.get_object(Bucket=source_bucket, Key=key)
            
            if key.endswith('csv'):
                df = pd.read_csv(io.BytesIO(file['Body'].read()))
            elif key.endswith('json'):
                df = pd.read_json(io.BytesIO(file['Body'].read()))
            
            # Convert to Parquet (in memory)
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=True)

            # Upload Parquet to destination S3
            s3_dest.put_object(
                Bucket=dest_bucket,
                Key=dest_key,
                Body=parquet_buffer.getvalue()
            )

            print(f'{dest_key} written to {dest_bucket} successfully!')
        
        print(f"{s3_key} data transfer to {dest_bucket} completed!!")
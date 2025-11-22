import boto3
import os
from dotenv import load_dotenv
from airflow.models import Variable

# load .env file
load_dotenv()

# create a boto session
def aws_session():
    '''
    A function to define boto session to be used across all AWS service (for data extraction)
    '''
    session = boto3.Session(
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
        region_name=Variable.get("AWS_REGION")
    )
    return session

# create a s3 client from boto session
session = aws_session()
s3 = session.client('s3')

# s3 bucket and keys
bucket_name = 'core-telecoms-data-lake'
s3_keys = ['call logs', 'customers', 'social_medias']
local_download_dir = '/opt/airflow/dags/include/s3_datasets'

def download_s3_files():
    '''
    A function to download all files from s3 bucket
    '''
    # Create the local download directory if it doesn't exist
    os.makedirs(local_download_dir, exist_ok=True)
    
    for s3_key in s3_keys:
        # create a local subfolder
        subfolder = os.path.join(local_download_dir, s3_key)
        os.makedirs(subfolder, exist_ok=True)

        # list all object in the s3 key
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=s3_key)
        for obj in response['Contents']:
            key = obj["Key"]
            if key.endswith('/'):
                continue

            filename = key.split('/')[-1]
            local_path = os.path.join(subfolder, filename)

            # check if the file already exist in the local_path - idempotency
            if os.path.exists(local_path):
                continue

            # download files from the s3 key
            s3.download_file(
                Bucket=bucket_name,
                Key=key,
                Filename=local_path
            )
        print(f'{s3_key} data downloaded successfully')
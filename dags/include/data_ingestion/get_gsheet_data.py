import gspread
import pandas as pd
import json
import io
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from include.data_ingestion.get_s3_files import get_boto3_client

hook = GCSHook(gcp_conn_id="google_cloud_default")
# client = hook.get_conn()
cred_dict = json.loads(hook._get_field("keyfile_dict"))

spreadsheet_key = '1JePJO9e3h6tEqldkJjD5P6__yEpzbOtshdPN1mgkqoE'

dest_bucket = 'cde-capstone-olalekan'
key_prefix = 'agents'

def _extract_gsheet_data():
    '''
    A function to extract data from a private google sheet
    '''

    # output parquet file
    dest_key = f'{key_prefix}/agents.parquet'

    s3_dest = get_boto3_client('aws_dest', 'us-east-1', 's3')
    objs = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix=key_prefix)
    objs_list = [obj['Key'] for obj in objs.get('Content', [])]

    if dest_key not in objs_list:

        # authenticate google spreadsheet with service account credentials
        gc = gspread.service_account_from_dict(cred_dict)

        # Open the Google Sheet by its key
        spreadsheet = gc.open_by_key(spreadsheet_key)

        # selecting worksheet by its title
        worksheet = spreadsheet.worksheet("agents")

        # convert google sheet data to pandas dataframe
        df = pd.DataFrame(worksheet.get_all_records())

        # Convert to Parquet (in memory)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', index=True)

        # Upload Parquet to destination S3
        s3_dest.put_object(
            Bucket=dest_bucket,
            Key=dest_key,
            Body=parquet_buffer.getvalue()
        )

        print("Agents data extraction completed!!")

    else:
        print(f"{dest_key} already exist in destination bucket {dest_bucket}..")
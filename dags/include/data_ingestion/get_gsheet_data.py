import gspread
import pandas as pd
import json
import io
from datetime import datetime
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from include.data_ingestion.get_s3_files import get_boto3_client

# set google cloud hook for service account authentication
hook = GCSHook(gcp_conn_id="google_cloud_default")
cred_dict = json.loads(hook._get_field("keyfile_dict"))

# spreadsheet key id
spreadsheet_key = '1JePJO9e3h6tEqldkJjD5P6__yEpzbOtshdPN1mgkqoE'

# s3 buckets parameter
dest_bucket = 'cde-capstone-olalekan'
key_prefix = 'agents'

def _extract_gsheet_data():
    '''
    A function to extract data from a private google sheet
    '''

    # key to be used in s3 bucket destination
    dest_key = f'{key_prefix}/agents.parquet'

    # list the existing keys in the agents s3 bucket subfolder
    s3_dest = get_boto3_client('aws_dest', 'us-east-1', 's3')
    objs = s3_dest.list_objects_v2(Bucket=dest_bucket, Prefix=key_prefix)
    objs_list = [obj['Key'] for obj in objs.get('Contents', [])]

    if dest_key not in objs_list:

        # authenticate google spreadsheet with service account credentials
        gc = gspread.service_account_from_dict(cred_dict)

        # Open the Google Sheet by its key
        spreadsheet = gc.open_by_key(spreadsheet_key)

        # selecting worksheet by its title
        worksheet = spreadsheet.worksheet("agents")

        # convert google sheet data to pandas dataframe
        df = pd.DataFrame(worksheet.get_all_records())

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
                "source_file": "Google Spreadsheet",
                "record_count": str(len(df)),
                "no_of_columns": str(len(df.columns))
            }
        )

        print("Agents data extraction completed!!")

    else:
        print(f"{dest_key} already exist in destination bucket {dest_bucket}..")
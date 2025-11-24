import gspread
import pandas as pd
import os
from dotenv import load_dotenv

# load environment variables
path_to_vars = '/opt/airflow/dags/include/extraction/aws_creds.env'
load_dotenv(path_to_vars)

# service account creds and google sheet key
cred_filepath = '/opt/airflow/dags/include/extraction/gc_credentials.json'
spreadsheet_key = os.getenv("spreadsheet_key")

# output file path
output_file_path = '/opt/airflow/dags/include/data_sources/agents'

def _extract_gsheet_data():
    '''
    A function to extract data from a private google sheet
    '''
    # create a sub-directory where the output data will be written to if not exist
    os.makedirs(output_file_path, exist_ok=True)
    
    # output parquet file
    filename = 'agents.parquet'
    output_file = os.path.join(output_file_path, filename)

    # authenticate google spreadsheet with service account credentials
    gc = gspread.service_account(filename=cred_filepath)

    # Open the Google Sheet by its key
    spreadsheet = gc.open_by_key(spreadsheet_key)

    # selecting worksheet by its title
    worksheet = spreadsheet.worksheet("agents")

    # convert google sheet data to pandas dataframe
    df = pd.DataFrame(worksheet.get_all_records())
    df.to_parquet(output_file)

    print("Agents data extraction completed!!")
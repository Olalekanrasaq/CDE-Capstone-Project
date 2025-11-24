from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from include.extraction.get_s3_files import download_s3_files
from include.extraction.get_website_complaints import copy_postgres_table
from include.extraction.get_gsheet_data import _extract_gsheet_data

with DAG(
    dag_id="coretelecom",
    start_date=datetime(2025, 11, 21),
    schedule=None
):
    
    extract_s3_files = PythonOperator(
        task_id="extract_s3_files",
        python_callable=download_s3_files
    )

    extract_db_tables = PythonOperator(
        task_id='extract_postgres_data',
        python_callable=copy_postgres_table
    )

    extract_gsheet_data = PythonOperator(
        task_id = 'extract_gsheet_data',
        python_callable=_extract_gsheet_data
    )

extract_s3_files >> extract_db_tables >> extract_gsheet_data
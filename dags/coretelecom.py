from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from include.data_ingestion.get_s3_files import transfer_s3_files
from include.data_ingestion.get_website_complaints import copy_postgres_table
from include.data_ingestion.get_gsheet_data import _extract_gsheet_data

with DAG(
    dag_id="coretelecom",
    start_date=datetime(2025, 11, 21),
    schedule=None
):
    
    # extract_s3_files = PythonOperator(
    #     task_id="extract_s3_files",
    #     python_callable=download_s3_files
    # )

    # extract_db_tables = PythonOperator(
    #     task_id='extract_postgres_data',
    #     python_callable=copy_postgres_table
    # )

    # extract_gsheet_data = PythonOperator(
    #     task_id = 'extract_gsheet_data',
    #     python_callable=_extract_gsheet_data
    # )

    ingest_s3_files = PythonOperator(
        task_id="ingest_s3_files",
        python_callable=transfer_s3_files
    )

    ingest_db_tables = PythonOperator(
        task_id='ingest_postgres_data',
        python_callable=copy_postgres_table
    )

    ingest_gsheet_data = PythonOperator(
        task_id = 'ingest_gsheet_data',
        python_callable=_extract_gsheet_data
    )



ingest_s3_files >> ingest_db_tables >> ingest_gsheet_data
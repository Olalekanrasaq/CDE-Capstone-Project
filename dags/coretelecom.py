from airflow.sdk import DAG
from pendulum import datetime
from datetime import timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from include.data_ingestion.get_s3_files import transfer_s3_files
from include.data_ingestion.get_website_complaints import copy_postgres_table
from include.data_ingestion.get_gsheet_data import _extract_gsheet_data
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="cde_capstone",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_conn",
        profile_args={"schema": "landing"},
    )
)

dbt_project_path = "/opt/airflow/dags/dbt_project"

with DAG(
    dag_id="coretelecom",
    start_date=datetime(2025, 11, 21),
    schedule=None
):

    ingest_s3_files = PythonOperator(
        task_id="ingest_s3_files",
        python_callable=transfer_s3_files
    )

    # ingest_db_tables = PythonOperator(
    #     task_id='ingest_postgres_data',
    #     python_callable=copy_postgres_table
    # )

    ingest_gsheet_data = PythonOperator(
        task_id='ingest_gsheet_data',
        python_callable=_extract_gsheet_data
    )

    wait_10_secs = TimeDeltaSensor(
        task_id='wait_10_seconds',
        delta=timedelta(seconds=10)
    )

    run_dbt_models = DbtTaskGroup(
        group_id="model_dw_data",
        project_config=ProjectConfig(dbt_project_path),
        profile_config=profile_config
    )

    [ingest_s3_files, ingest_gsheet_data] >> wait_10_secs >> run_dbt_models
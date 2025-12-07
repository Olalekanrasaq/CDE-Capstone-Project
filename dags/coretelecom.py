from airflow.sdk import DAG
from pendulum import datetime
from datetime import timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from include.data_ingestion.get_s3_files import transfer_s3_files
from include.data_ingestion.get_website_complaints import copy_postgres_table
from include.data_ingestion.get_gsheet_data import _extract_gsheet_data
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping

# configure dbt profile
profile_config = ProfileConfig(
    profile_name="dbt_project",
    target_name="cde_capstone",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id="redshift_conn",
        profile_args={"schema": "landing"},
    )
)

# path to dbt project directory
dbt_project_path = "/opt/airflow/dags/dbt_project"

# define task success and failure callbacks and log to a file
def success_callback(context):
    with open("/opt/airflow/logs/task_events.log", "a") as f:
        f.write(f"{datetime.now()} SUCCESS: {context['task_instance'].task_id}\n")

def failure_callback(context):
    with open("/opt/airflow/logs/task_events.log", "a") as f:
        f.write(f"{datetime.now()} FAILED: {context['task_instance'].task_id}\n")

# define default args for the DAG
default_args={"owner": "Olalekan-CDE-Capstone",
              "email": ["olalekanrasaq1331@gmail.com"],
              "email_on_failure": True,
              "email_on_retry": False,
              "retries": 1, 
              "retry_delay": timedelta(minutes=1),
              "on_success_callback": success_callback,
              "on_failure_callback": failure_callback
              }


# define the DAG
with DAG(
    dag_id="coretelecom",
    start_date=datetime(2025, 11, 21),
    schedule=None,
    default_args=default_args
):
    # Ingest s3 files to Redshift
    ingest_s3_files = PythonOperator(
        task_id="ingest_s3_files",
        python_callable=transfer_s3_files
    )

    # Ingest web form complaints data in Postgres tables to Redshift
    ingest_db_tables = PythonOperator(
        task_id='ingest_postgres_data',
        python_callable=copy_postgres_table
    )

    # Ingest Google Sheets data to Redshift
    ingest_gsheet_data = PythonOperator(
        task_id='ingest_gsheet_data',
        python_callable=_extract_gsheet_data
    )

    # Wait for 10 seconds before running dbt models
    wait_10_secs = TimeDeltaSensor(
        task_id='wait_10_seconds',
        delta=timedelta(seconds=10)
    )

    # Run dbt models to transform data in Redshift
    run_dbt_models = DbtTaskGroup(
        group_id="model_dw_data",
        project_config=ProjectConfig(dbt_project_path),
        profile_config=profile_config,
        default_args={"retries": 2}
    )

    # Define task dependencies
    [ingest_s3_files, ingest_db_tables, ingest_gsheet_data] >> wait_10_secs >> run_dbt_models
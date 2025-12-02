from airflow.sdk import DAG
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from include.data_ingestion.jobs_drop import delete_copy_job

with DAG(
    dag_id="initialize_redshift_autocopy",
    start_date=datetime(2025, 11, 27),
    description="A dag that create redshift database schemas and initialize s3 auto copy.",
    schedule=None
):

    create_redshift_schemas = RedshiftDataOperator(
        task_id='create_redshift_schemas',
        cluster_identifier='cde-capstone-cluster',
        database='cde_capstone',
        db_user='olalekanrasaq',
        region_name='us-east-1',
        aws_conn_id='aws_dest',
        sql='include/sql/redshift_schemas.sql'
    )

    copy_jobs = (
        'cde_s3_auto_copy_customers', 
        'cde_s3_auto_copy_agents', 
        'cde_s3_auto_copy_calllogs',
        'cde_s3_auto_copy_socialmedia', 
        'cde_s3_auto_copy_websiteforms'
        )

    drop_copy_jobs = PythonOperator(
        task_id='drop_exist_copy_jobs',
        python_callable=delete_copy_job,
        op_kwargs={"jobs_to_drop": copy_jobs}
    )

    load_redshift_tables = RedshiftDataOperator(
        task_id='auto_copy_s3_redshift',
        cluster_identifier='cde-capstone-cluster',
        database='cde_capstone',
        db_user='olalekanrasaq',
        region_name='us-east-1',
        aws_conn_id='aws_dest',
        sql='include/sql/data_load.sql'
    )
    
    trigger_elt_dag = TriggerDagRunOperator(
        task_id='trigger_elt_dag',
        trigger_dag_id='coretelecom'
    )

    create_redshift_schemas >> drop_copy_jobs >> load_redshift_tables >> trigger_elt_dag
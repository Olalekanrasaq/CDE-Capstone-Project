# Data Transformation and Loading with DBT

This directory contains the DBT (Data Build Tool) project for transforming and loading data into the Redshift data warehouse as part of the Coretelecom Unified Customer Experience Data Platform.


## DBT Project Structure
- `dbt_project/models/`: Contains DBT models that define the transformations and aggregations applied to the raw data.
    - `dbt_project/models/staging/`: Staging models that clean and prepare raw data for further transformations.
    - `dbt_project/models/prod/`: Data marts that aggregate and structure complaints data for downstream consumption.
- `dbt_project/macros/generate_schema_name.sql`: Custom macros to generate schema names dynamically. 

## Running DBT Models

To run the DBT models and transform data in Redshift, follow these steps:

1. **Navigate to the DBT Project Directory**: Change your working directory to the `dbt_project/` folder.
   
   ```bash
   cd dags/dbt_project/
   ```
2. **Install DBT Dependencies**: Ensure that you have DBT installed in your environment. You can install it using pip if you haven't done so already.
   
   ```bash
   pip install dbt-core
   pip install dbt-redshift
   ```
3. **Configure DBT Profile**: Set up your DBT profile to connect to your Redshift cluster. This is already done in our Airflow DAG using the `redshift_conn` connection.

4. **Run DBT Models**: The DBT models are executed as part of the Airflow DAG (`coretelecom.py`). When the DAG runs, it will automatically trigger the DBT transformations after data ingestion is complete.

# Design and Implementation of Coretelecom Unified Customer Experience Data Platform

## Project Overview

This project involves the design and implementation of a data platform for Coretelecom's Unified Customer Experience. The platform integrates data from various sources, including customer information, agent details, call logs, media complaints, and web form submissions. The data is processed and stored in a Redshift data warehouse, enabling advanced analytics and reporting capabilities.

## Project Structure
- `dags/`: Contains Airflow DAGs and SQL scripts for data ingestion and transformation.
  - `dags/coretelecom.py`: Airflow DAG for orchestrating the data pipeline.
  - `dags/initialize_autocopy.py`: Airflow DAG to initialize Redshift AutoCopy for efficient data loading and trigger coretelecom DAG.
  - `include/data_ingestion/`: Python scripts for ingesting data from various sources.
  - `include/sql/`: SQL scripts for creating Redshift schemas.
  - `dbt_project/models/`: DBT models for transforming and aggregating data.
- `infrastructure/`: Infrastructure as Code (IaC) scripts for setting up AWS resources using Terraform.
- `Dockerfile`: Docker configuration for the custom image.
- `requirements.txt`: Python dependencies for the project.
- `Docker-compose.yml`: Docker Compose configuration for local development and testing.

## Architecture Diagram

![Architecture Diagram](./infrastructure/images/coretelecom_architecture_diagram.png)

## Getting Started

To set up and run the Coretelecom Unified Customer Experience Data Platform, follow these steps:
    1. **Clone the Repository**: Clone this repository to your local machine.
    2. **Set Up AWS Resources**: Use the Terraform scripts in the `infrastructure/` directory to provision the necessary AWS resources. The AWS credentials should be configured in your environment. You can leverage `aws configure` to set up your credentials.
    3. **Build Docker Image**: Build the custom Docker image using the provided `Dockerfile`.
    4. **Configure Airflow**: Set up Airflow and configure the connections to AWS services.
    5. **Run Airflow DAGs**: 
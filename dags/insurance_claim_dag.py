import os
from dotenv import load_dotenv
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
from include.ingestion.dev_main import dev_main
from include.ingestion.bigquery_datasets import create_bq_dataset 

load_dotenv()

args = {
    'owner': 'abdul-dev'
}

# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')



source_path='https://raw.githubusercontent.com/princeabdul99/practice-dataset/refs/heads/main/insurance_claims'
SOURCE_FILES = [
    "brokers.csv",
    "coverages.csv",
    "participants.csv",
    "policies.csv",
    "products.csv",
    "regions.csv",
    "state_regions.csv",
    "claims_announcement.csv",
    "claims_payment.csv",
    "claims_reserves.csv",
]

SOURCE_FILEPATHS = [f"{source_path}/{file}" for file in SOURCE_FILES]

# Dataset
dev_datasets = ['ic_dev_bronze', 'ic_dev_silver', 'ic_dev_gold']
location = 'US'



@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    doc_md=__doc__,
    tags=["insurance claims"],
    default_args = args,
)

def insurance_claims_pipeline():
    external_file_to_gcs = PythonOperator.partial(
        task_id = "external_file_to_gcs",
        python_callable=dev_main,
    ).expand(
        op_kwargs=[{"source_filepath": p} for p in SOURCE_FILEPATHS]
    )

    create_datasets = PythonOperator(
        task_id = "create_datasets",
        python_callable=create_bq_dataset,
        op_kwargs={
           "project_id": GCP_PROJECT_ID,
           "datasets": dev_datasets, 
        }
    )

    external_file_to_gcs >> create_datasets


insurance_claims_pipeline()    

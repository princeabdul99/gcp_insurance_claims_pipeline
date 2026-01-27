import os
from dotenv import load_dotenv
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from pendulum import datetime
from airflow.models import Variable
from include.ingestion.dev_main import dev_main
from include.ingestion.bigquery_datasets import create_bq_dataset
from plugins.helper import execute_sql

load_dotenv()

args = {
    'owner': 'abdul-dev'
}

# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')

# Airflow Variables
settings = Variable.get("insurance_claims_dag_settings", deserialize_json=True)

# DAG Variables
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_bronze_dataset = settings['dev_bronze_dataset']
bq_silver_dataset = settings['dev_silver_dataset']
bq_gold_dataset = settings['dev_gold_dataset']

# Environment (dev/prod)
target_env = 'dev'

# Dataset
dev_datasets = ['ic_dev_bronze', 'ic_dev_silver', 'ic_dev_gold']
location = 'US'

# External Source Data
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

# bronze external table 
PATH_TO_DATA_SCRIPT = "/usr/local/airflow/include/ingestion/bronze_ext_tbl.sql"
dev_bronze_ext_table = execute_sql(f"{PATH_TO_DATA_SCRIPT}")

# ==== BROKER DATA ========
## == source (bucket)
gcs_brokers_source_object = "brokers/brokers.csv"
gcs_brokers_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_brokers_source_object}"
## == bronze layer (bigquery)
bq_brokers_table_name = "brokers"
bq_bronze_brokers_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_brokers_table_name}"

# ==== COVERAGE DATA ========
## == source (bucket)
gcs_coverages_source_object = "coverages/coverages.csv"
gcs_coverages_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_coverages_source_object}"
## == bronze layer (bigquery)
bq_coverages_table_name = "coverages"
bq_bronze_coverages_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_coverages_table_name}"

# ==== PARTICIPANTS DATA ========
## == source (bucket)
gcs_participants_source_object = "participants/participants.csv"
gcs_participants_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_participants_source_object}"
## == bronze layer (bigquery)
bq_participants_table_name = "participants"
bq_bronze_participants_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_participants_table_name}"

# ==== POLICIES DATA ========
## == source (bucket)
gcs_policies_source_object = "policies/policies.csv"
gcs_policies_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_policies_source_object}"
## == bronze layer (bigquery)
bq_policies_table_name = "policies"
bq_bronze_policies_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_policies_table_name}"

# ==== PRODUCT DATA ========
## == source (bucket)
gcs_products_source_object = "products/products.csv"
gcs_products_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_products_source_object}"
## == bronze layer (bigquery)
bq_products_table_name = "products"
bq_bronze_products_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_products_table_name}"

# ==== REGIONS DATA ========
## == source (bucket)
gcs_regions_source_object = "regions/regions.csv"
gcs_regions_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_regions_source_object}"
## == bronze layer (bigquery)
bq_regions_table_name = "regions"
bq_bronze_regions_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_regions_table_name}"

# ==== STATE REGIONS DATA ========
## == source (bucket)
gcs_state_regions_source_object = "state_regions/state_regions.csv"
gcs_state_regions_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_state_regions_source_object}"
## == bronze layer (bigquery)
bq_state_regions_table_name = "state_regions"
bq_bronze_state_regions_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_state_regions_table_name}"

# ==== CLAIM ANNOUNCEMENT DATA ========
## == source (bucket)
gcs_claims_announcement_source_object = "claims_announcement/claims_announcement.csv"
gcs_claims_announcement_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_claims_announcement_source_object}"
## == bronze layer (bigquery)
bq_claims_announcement_table_name = "claims_announcement"
bq_bronze_claims_announcement_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_claims_announcement_table_name}"

# ==== CLAIMS PAYMENT DATA ========
## == source (bucket)
gcs_claims_payment_source_object = "claims_payment/claims_payment.csv"
gcs_claims_payment_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_claims_payment_source_object}"
## == bronze layer (bigquery)
bq_claims_payment_table_name = "claims_payment"
bq_bronze_claims_payment_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_claims_payment_table_name}"

# ==== CLAIMS RESERVES DATA ========
## == source (bucket)
gcs_claims_reserves_source_object = "claims_reserves/claims_reserves.csv"
gcs_claims_reserves_source_url = f"gs://{gcs_source_data_bucket}/{target_env}/{gcs_claims_reserves_source_object}"
## == bronze layer (bigquery)
bq_claims_reserves_table_name = "claims_reserves"
bq_bronze_claims_reserves_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_claims_reserves_table_name}"



@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    doc_md=__doc__,
    tags=["insurance claims"],
    default_args = args,
)


def insurance_claims_pipeline():

    # INGEST DATA FROM EXTERNAL SOURCE TO GCS BUCKET
    external_file_to_gcs = PythonOperator.partial(
        task_id = "external_file_to_gcs",
        python_callable=dev_main,
    ).expand(
        op_kwargs=[{"source_filepath": p} for p in SOURCE_FILEPATHS]
    )

    # CREATE DATASETS IN BIGQUERY
    create_datasets = PythonOperator(
        task_id = "create_datasets",
        python_callable=create_bq_dataset,
        op_kwargs={
           "project_id": GCP_PROJECT_ID,
           "datasets": dev_datasets, 
        }
    )
    
    # LOAD BQ BRONZE LAYER FROM GCS BUCKET 
    ### -- Brokers data ----
    gcs_to_bq_bronze_brokers_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_brokers_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_brokers_table_name,
            "gcs_uri": gcs_brokers_source_url,
            "skip_leading_rows": 1
        }
    ) 

    ### -- Coverages data ----
    gcs_to_bq_bronze_coverages_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_coverages_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_coverages_table_name,
            "gcs_uri": gcs_coverages_source_url,
            "skip_leading_rows": 1
        }
    ) 

    ### -- Participant data ----
    gcs_to_bq_bronze_participants_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_participants_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_participants_table_name,
            "gcs_uri": gcs_participants_source_url,
            "skip_leading_rows": 1
        }
    ) 

    ### -- Policy data ----
    gcs_to_bq_bronze_policies_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_policies_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_policies_table_name,
            "gcs_uri": gcs_policies_source_url,
            "skip_leading_rows": 1
        }
    )     

    ### -- Products data ----
    gcs_to_bq_bronze_products_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_products_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_products_table_name,
            "gcs_uri": gcs_products_source_url,
            "skip_leading_rows": 1
        }
    )     

    ### -- Regions data ----
    gcs_to_bq_bronze_regions_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_regions_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_regions_table_name,
            "gcs_uri": gcs_regions_source_url,
            "skip_leading_rows": 1
        }
    ) 

    ### -- State Regions data ----
    gcs_to_bq_bronze_state_regions_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_state_regions_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_state_regions_table_name,
            "gcs_uri": gcs_state_regions_source_url,
            "skip_leading_rows": 1
        }
    ) 

    ### -- Claims Announcements data ----
    gcs_to_bq_bronze_claims_announcement_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_claims_announcement_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_claims_announcement_table_name,
            "gcs_uri": gcs_claims_announcement_source_url,
            "skip_leading_rows": 1
        }
    )   

    ### -- Claims Payment data ----
    gcs_to_bq_bronze_claims_payment_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_claims_payment_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_claims_payment_table_name,
            "gcs_uri": gcs_claims_payment_source_url,
            "skip_leading_rows": 1
        }
    )          

    ### -- Claims Reserve data ----
    gcs_to_bq_bronze_claims_reserves_ext_tbl = BigQueryInsertJobOperator(
        task_id = "gcs_to_bq_bronze_claims_reserves_ext_tbl",
        project_id = GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": dev_bronze_ext_table,
                "useLegacySql": False
            }
        },
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset": bq_bronze_dataset,
            "table_name": bq_claims_reserves_table_name,
            "gcs_uri": gcs_claims_reserves_source_url,
            "skip_leading_rows": 1
        }
    )  
    ### END LOAD BQ BRONZE LAYER FROM GCS BUCKET <=;



    external_file_to_gcs >> create_datasets 
    [create_datasets] >> gcs_to_bq_bronze_brokers_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_coverages_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_participants_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_policies_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_products_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_regions_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_state_regions_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_claims_announcement_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_claims_payment_ext_tbl
    [create_datasets] >> gcs_to_bq_bronze_claims_reserves_ext_tbl

insurance_claims_pipeline()    

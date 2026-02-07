import os
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator
from pendulum import datetime
from include.ingestion.dev_main import dev_main
from include.ingestion import gcs_bucket_data_source as ds_gcs
from include.ingestion.bigquery_datasets import create_bq_dataset
from plugins.helper import dev_bronze_ext_table
from include.dbt.cosmos_config import target_env, DBT_PROJECT_CONFIG, DBT_PROFILE_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos import RenderConfig





# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')



# Dataset
# dev_datasets = ['ic_dev_bronze', 'ic_dev_silver', 'ic_dev_gold']
dev_datasets = [f"ic_{target_env}_bronze", f"ic_{target_env}_silver", f"ic_{target_env}_gold"]
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

args = {
    'owner': 'abdul-dev'
}

@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    doc_md=__doc__,
    tags=["insurance claims"],
    default_args = args,
)


def insurance_claims_pipeline():

    # INGEST DATA FROM EXTERNAL SOURCE TO GCS BUCKET
    external_file_to_gcs_dev = PythonOperator.partial(
        task_id = "external_file_to_gcs_dev",
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_brokers_table_name,
            "gcs_uri": ds_gcs.gcs_brokers_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_coverages_table_name,
            "gcs_uri": ds_gcs.gcs_coverages_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_participants_table_name,
            "gcs_uri": ds_gcs.gcs_participants_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_policies_table_name,
            "gcs_uri": ds_gcs.gcs_policies_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_products_table_name,
            "gcs_uri": ds_gcs.gcs_products_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_regions_table_name,
            "gcs_uri": ds_gcs.gcs_regions_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_state_regions_table_name,
            "gcs_uri": ds_gcs.gcs_state_regions_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_claims_announcement_table_name,
            "gcs_uri": ds_gcs.gcs_claims_announcement_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_claims_payment_table_name,
            "gcs_uri": ds_gcs.gcs_claims_payment_source_url,
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
            "dataset": ds_gcs.bq_bronze_dataset,
            "table_name": ds_gcs.bq_claims_reserves_table_name,
            "gcs_uri": ds_gcs.gcs_claims_reserves_source_url,
            "skip_leading_rows": 1
        }
    )  
    ### END LOAD BQ BRONZE LAYER FROM GCS BUCKET <=;

    dbt_test_raw = BashOperator(
        task_id='dbt_test_raw',
        bash_command="dbt test --select tag:silver",
        cwd="/usr/local/airflow/include/dbt"
    )

    dbt_bronze_to_silver = BashOperator(
        task_id='dbt_bronze_to_silver',
        bash_command="dbt run --select tag:silver",
        cwd="/usr/local/airflow/include/dbt"
    )

    dbt_silver_to_gold = BashOperator(
        task_id='dbt_silver_to_gold',
        bash_command="dbt run --select tag:gold",
        cwd="/usr/local/airflow/include/dbt"
    )
    

    # dbt_bronze_to_silver = DbtTaskGroup(
    #     group_id="dbt_bronze_to_silver",
    #     project_config=DBT_PROJECT_CONFIG,
    #     profile_config=DBT_PROFILE_CONFIG,
    #     render_config=RenderConfig(
    #         load_method=LoadMode.DBT_LS,
    #         select=["tag:silver"],
    #         dbt_executable_path="/usr/local/airflow/include/dbt" 
    #     ),
    #     operator_args={
    #         "install_deps": True,
    #         "emit_openlineage_events": False,
    #         # "emit_datasets": False,
    #     },
    # )

    ######## BQ Row Count Checker
    bq_row_count_check_on_silver_brokers = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_brokers',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_brokers_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_coverages = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_coverages',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_coverages_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_participants = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_participants',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_participants_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_policies = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_policies',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_policies_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_products = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_products',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_products_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_regions = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_regions',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_regions_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_state_regions = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_state_regions',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_state_regions_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_claims_announcement = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_claims_announcement',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_claims_announcement_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_claims_payment = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_claims_payment',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_claims_payment_table_name}`
            """,
        use_legacy_sql=False,
    )

    bq_row_count_check_on_silver_claims_reserves = BigQueryCheckOperator(
        task_id='bq_row_count_check_on_silver_claims_reserves',
        sql=f"""
            SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_silver_dataset}.{ds_gcs.bq_claims_reserves_table_name}`
            """,
        use_legacy_sql=False,
    )

    ######### END Row Checker

    external_file_to_gcs_dev >> create_datasets 
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

    bronze_tasks = [
    gcs_to_bq_bronze_brokers_ext_tbl
   ,gcs_to_bq_bronze_coverages_ext_tbl
   ,gcs_to_bq_bronze_participants_ext_tbl
   ,gcs_to_bq_bronze_policies_ext_tbl
   ,gcs_to_bq_bronze_products_ext_tbl
   ,gcs_to_bq_bronze_regions_ext_tbl
   ,gcs_to_bq_bronze_state_regions_ext_tbl
   ,gcs_to_bq_bronze_claims_announcement_ext_tbl
   ,gcs_to_bq_bronze_claims_payment_ext_tbl
   ,gcs_to_bq_bronze_claims_reserves_ext_tbl] 
    
    bronze_tasks >> dbt_test_raw >> dbt_bronze_to_silver

    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_brokers
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_coverages
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_participants
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_policies
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_products
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_regions
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_state_regions
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_claims_announcement
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_claims_payment
    [dbt_bronze_to_silver] >> bq_row_count_check_on_silver_claims_reserves

    silver_table_check = [
        bq_row_count_check_on_silver_brokers
        ,bq_row_count_check_on_silver_coverages
        ,bq_row_count_check_on_silver_participants
        ,bq_row_count_check_on_silver_policies
        ,bq_row_count_check_on_silver_products
        ,bq_row_count_check_on_silver_regions
        ,bq_row_count_check_on_silver_state_regions
        ,bq_row_count_check_on_silver_claims_announcement
        ,bq_row_count_check_on_silver_claims_payment
        ,bq_row_count_check_on_silver_claims_reserves        
    ]

    silver_table_check >> dbt_silver_to_gold

insurance_claims_pipeline()




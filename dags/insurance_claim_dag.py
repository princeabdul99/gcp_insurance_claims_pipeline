import os
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator 
from pendulum import datetime
from include.ingestion.dev_main import dev_main
from include.ingestion.prod_main import prod_main
from include.ingestion import gcs_bucket_data_source as ds_gcs
from include.ingestion.bigquery_datasets import create_bq_dataset
from include.dbt.cosmos_config import target_env


# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('BUCKET_NAME')



# Dataset
bq_datasets = [f"ic_{target_env}_bronze", f"ic_{target_env}_silver", f"ic_{target_env}_gold"]
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
    schedule="@daily",
    doc_md=__doc__,
    tags=["insurance claims"],
    default_args = args,
    catchup=False,
)


def insurance_claims_pipeline():

    # INGEST DATA FROM EXTERNAL SOURCE TO GCS BUCKET
    external_file_to_gcs = PythonOperator.partial(
        task_id = f"external_file_to_gcs_{target_env}",
        python_callable=dev_main if target_env == "dev" else prod_main
    ).expand(
        op_kwargs=[{"source_filepath": p} for p in SOURCE_FILEPATHS]
    )

    # CREATE DATASETS IN BIGQUERY
    create_datasets = PythonOperator(
        task_id = f"create_datasets_{target_env}",
        python_callable=create_bq_dataset,
        op_kwargs={
           "project_id": GCP_PROJECT_ID,
           "datasets": bq_datasets, 
        }
    )
    
    # LOAD BQ BRONZE LAYER FROM GCS BUCKET 
    ### -- Brokers data ----
    gcs_to_bq_bronze_brokers = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_brokers_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_brokers_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_brokers_table_id,
        schema_fields = ds_gcs.brokers_schema,
        write_disposition='WRITE_TRUNCATE'
    )

    ### -- Coverages data ----
    gcs_to_bq_bronze_coverages = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_coverages_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_coverages_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_coverages_table_id,
        schema_fields = ds_gcs.coverages_schema,
        write_disposition='WRITE_TRUNCATE'
    )    

    ### -- Participant data ----
    gcs_to_bq_bronze_participants = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_participants_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_participants_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_participants_table_id,
        schema_fields = ds_gcs.participants_schema,
        write_disposition='WRITE_TRUNCATE'
    )

    ### -- Policy data ----
    gcs_to_bq_bronze_policies = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_policies_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_policies_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_policies_table_id,
        schema_fields = ds_gcs.policies_schema,
        write_disposition='WRITE_TRUNCATE'
    )    

    ### -- Products data ----
    gcs_to_bq_bronze_products = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_products_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_products_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_products_table_id,
        schema_fields = ds_gcs.products_schema,
        write_disposition='WRITE_TRUNCATE'
    )    

    ### -- Regions data ----
    gcs_to_bq_bronze_regions = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_regions_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_regions_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_regions_table_id,
        schema_fields = ds_gcs.regions_schema,
        write_disposition='WRITE_TRUNCATE'
    )

    ### -- State Regions data ----
    gcs_to_bq_bronze_state_regions = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_state_regions_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_state_regions_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_state_regions_table_id,
        schema_fields = ds_gcs.state_regions_schema,
        write_disposition='WRITE_TRUNCATE'
    ) 

    ### -- Claims Announcements data ----
    gcs_to_bq_bronze_claims_announcement = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_claims_announcement_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_claims_announcement_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_claims_announcement_table_id,
        schema_fields = ds_gcs.claims_announcement_schema,
        write_disposition='WRITE_TRUNCATE'
    ) 

    ### -- Claims Payment data ----
    gcs_to_bq_bronze_claims_payment = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_claims_payment_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_claims_payment_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_claims_payment_table_id,
        schema_fields = ds_gcs.claims_payment_schema,
        write_disposition='WRITE_TRUNCATE'
    )         

    ### -- Claims Reserve data ----
    gcs_to_bq_bronze_claims_reserves = GCSToBigQueryOperator(
        task_id = f"gcs_to_bq_bronze_claims_reserves_{target_env}",
        bucket='{}'.format(GCS_BUCKET_NAME),
        external_table=True,
        source_format="CSV",
        skip_leading_rows=1,
        source_objects=[ds_gcs.gcs_claims_reserves_source_object],
        destination_project_dataset_table=ds_gcs.bq_bronze_claims_reserves_table_id,
        schema_fields = ds_gcs.claims_reserves_schema,
        write_disposition='WRITE_TRUNCATE'
    ) 
    ### END LOAD BQ BRONZE LAYER FROM GCS BUCKET <=;
    
    if target_env == "dev":
        ######## BQ Bronze Row Count Checker
        bq_row_count_check_on_bronze_brokers = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_brokers_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_brokers_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_coverages = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_coverages_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_coverages_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_participants = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_participants_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_participants_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_policies = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_policies_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_policies_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_products = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_products_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_products_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_regions = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_regions_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_regions_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_state_regions = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_state_regions_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_state_regions_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_claims_announcement = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_claims_announcement_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_claims_announcement_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_claims_payment = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_claims_payment_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_claims_payment_table_name}`
                """,
            use_legacy_sql=False,
        )

        bq_row_count_check_on_bronze_claims_reserves = BigQueryCheckOperator(
            task_id=f'bq_row_count_check_on_bronze_claims_reserves_{target_env}',
            sql=f"""
                SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{ds_gcs.bq_bronze_dataset}.{ds_gcs.bq_claims_reserves_table_name}`
                """,
            use_legacy_sql=False,
        )

        ######### END Bronze Row Checker

        
        external_file_to_gcs >> create_datasets 
        [create_datasets] >> gcs_to_bq_bronze_brokers >> bq_row_count_check_on_bronze_brokers
        [create_datasets] >> gcs_to_bq_bronze_coverages >> bq_row_count_check_on_bronze_coverages
        [create_datasets] >> gcs_to_bq_bronze_participants >> bq_row_count_check_on_bronze_participants
        [create_datasets] >> gcs_to_bq_bronze_policies >> bq_row_count_check_on_bronze_policies
        [create_datasets] >> gcs_to_bq_bronze_products >> bq_row_count_check_on_bronze_products
        [create_datasets] >> gcs_to_bq_bronze_regions >> bq_row_count_check_on_bronze_regions
        [create_datasets] >> gcs_to_bq_bronze_state_regions >> bq_row_count_check_on_bronze_state_regions
        [create_datasets] >> gcs_to_bq_bronze_claims_announcement >> bq_row_count_check_on_bronze_claims_announcement
        [create_datasets] >> gcs_to_bq_bronze_claims_payment >> bq_row_count_check_on_bronze_claims_payment
        [create_datasets] >> gcs_to_bq_bronze_claims_reserves >> bq_row_count_check_on_bronze_claims_reserves
    else:
        external_file_to_gcs >> create_datasets 
        [create_datasets] >> gcs_to_bq_bronze_brokers 
        [create_datasets] >> gcs_to_bq_bronze_coverages 
        [create_datasets] >> gcs_to_bq_bronze_participants 
        [create_datasets] >> gcs_to_bq_bronze_policies 
        [create_datasets] >> gcs_to_bq_bronze_products 
        [create_datasets] >> gcs_to_bq_bronze_regions 
        [create_datasets] >> gcs_to_bq_bronze_state_regions 
        [create_datasets] >> gcs_to_bq_bronze_claims_announcement 
        [create_datasets] >> gcs_to_bq_bronze_claims_payment 
        [create_datasets] >> gcs_to_bq_bronze_claims_reserves 


    dbt_bronze_to_silver = BashOperator(
        task_id=f'dbt_bronze_to_silver_{target_env}',
        bash_command="dbt run --select tag:silver",
        cwd="/usr/local/airflow/include/dbt"
    )
    

    dbt_silver_to_gold = BashOperator(
        task_id=f'dbt_silver_to_gold_{target_env}',
        bash_command="dbt run --select tag:gold",
        cwd="/usr/local/airflow/include/dbt"
    )

    if target_env == "dev":
        dbt_test_silver_executed = BashOperator(
            task_id=f'dbt_test_silver_executed_{target_env}',
            bash_command="dbt test --select tag:silver",
            cwd="/usr/local/airflow/include/dbt"
        )

        bronze_tasks_check = [
        bq_row_count_check_on_bronze_brokers
        ,bq_row_count_check_on_bronze_coverages
        ,bq_row_count_check_on_bronze_participants
        ,bq_row_count_check_on_bronze_policies
        ,bq_row_count_check_on_bronze_products
        ,bq_row_count_check_on_bronze_regions
        ,bq_row_count_check_on_bronze_state_regions
        ,bq_row_count_check_on_bronze_claims_announcement
        ,bq_row_count_check_on_bronze_claims_payment
        ,bq_row_count_check_on_bronze_claims_reserves] 
        
        bronze_tasks_check >> dbt_bronze_to_silver >> dbt_test_silver_executed >> dbt_silver_to_gold
    
    else:
        [gcs_to_bq_bronze_brokers 
        ,gcs_to_bq_bronze_coverages 
        ,gcs_to_bq_bronze_participants 
        ,gcs_to_bq_bronze_policies 
        ,gcs_to_bq_bronze_products 
        ,gcs_to_bq_bronze_regions 
        ,gcs_to_bq_bronze_state_regions 
        ,gcs_to_bq_bronze_claims_announcement 
        ,gcs_to_bq_bronze_claims_payment 
        ,gcs_to_bq_bronze_claims_reserves] >> dbt_bronze_to_silver >> dbt_silver_to_gold


insurance_claims_pipeline()




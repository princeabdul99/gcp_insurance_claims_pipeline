import os
from airflow.models import Variable
from include.dbt.cosmos_config import target_env
from plugins.helper import read_json_schema

# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')

# Airflow Variables
settings = Variable.get("insurance_claims_dag_settings", deserialize_json=True)

# DAG Variables
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_bronze_dataset = settings['dev_bronze_dataset'] if target_env == "dev" else settings['prod_bronze_dataset']
bq_silver_dataset = settings['dev_silver_dataset'] if target_env == "dev" else settings['dev_silver_dataset']
# bq_gold_dataset = settings['dev_gold_dataset']      
# 

PATH_TO_SCHEMAS_SCRIPT = "/usr/local/airflow/include/schema"

# ==== BROKER DATA ========
## == source (bucket)
gcs_brokers_source_object = f"{target_env}/brokers/brokers.csv"
gcs_brokers_source_url = f"gs://{gcs_source_data_bucket}/{gcs_brokers_source_object}"
## == bronze layer (bigquery)
bq_brokers_table_name = "brokers"
bq_bronze_brokers_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_brokers_table_name}"
brokers_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/brokers.json")

# ==== COVERAGE DATA ========
## == source (bucket)
gcs_coverages_source_object = f"{target_env}/coverages/coverages.csv"
gcs_coverages_source_url = f"gs://{gcs_source_data_bucket}/{gcs_coverages_source_object}"
## == bronze layer (bigquery)
bq_coverages_table_name = "coverages"
bq_bronze_coverages_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_coverages_table_name}"
coverages_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/coverages.json")

# ==== PARTICIPANTS DATA ========
## == source (bucket)
gcs_participants_source_object = f"{target_env}/participants/participants.csv"
gcs_participants_source_url = f"gs://{gcs_source_data_bucket}/{gcs_participants_source_object}"
## == bronze layer (bigquery)
bq_participants_table_name = "participants"
bq_bronze_participants_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_participants_table_name}"
participants_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/participants.json")

# ==== POLICIES DATA ========
## == source (bucket)
gcs_policies_source_object = f"{target_env}/policies/policies.csv"
gcs_policies_source_url = f"gs://{gcs_source_data_bucket}/{gcs_policies_source_object}"
## == bronze layer (bigquery)
bq_policies_table_name = "policies"
bq_bronze_policies_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_policies_table_name}"
policies_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/policies.json")

# ==== PRODUCT DATA ========
## == source (bucket)
gcs_products_source_object = f"{target_env}/products/products.csv"
gcs_products_source_url = f"gs://{gcs_source_data_bucket}/{gcs_products_source_object}"
## == bronze layer (bigquery)
bq_products_table_name = "products"
bq_bronze_products_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_products_table_name}"
products_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/products.json")

# ==== REGIONS DATA ========
## == source (bucket)
gcs_regions_source_object = f"{target_env}/regions/regions.csv"
gcs_regions_source_url = f"gs://{gcs_source_data_bucket}/{gcs_regions_source_object}"
## == bronze layer (bigquery)
bq_regions_table_name = "regions"
bq_bronze_regions_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_regions_table_name}"
regions_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/regions.json")

# ==== STATE REGIONS DATA ========
## == source (bucket)
gcs_state_regions_source_object = f"{target_env}/state_regions/state_regions.csv"
gcs_state_regions_source_url = f"gs://{gcs_source_data_bucket}/{gcs_state_regions_source_object}"
## == bronze layer (bigquery)
bq_state_regions_table_name = "state_regions"
bq_bronze_state_regions_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_state_regions_table_name}"
state_regions_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/state_regions.json")

# ==== CLAIM ANNOUNCEMENT DATA ========
## == source (bucket)
gcs_claims_announcement_source_object = f"{target_env}/claims_announcement/claims_announcement.csv"
gcs_claims_announcement_source_url = f"gs://{gcs_source_data_bucket}/{gcs_claims_announcement_source_object}"
## == bronze layer (bigquery)
bq_claims_announcement_table_name = "claims_announcement"
bq_bronze_claims_announcement_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_claims_announcement_table_name}"
claims_announcement_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/claims_announcement.json")

# ==== CLAIMS PAYMENT DATA ========
## == source (bucket)
gcs_claims_payment_source_object = f"{target_env}/claims_payment/claims_payment.csv"
gcs_claims_payment_source_url = f"gs://{gcs_source_data_bucket}/{gcs_claims_payment_source_object}"
## == bronze layer (bigquery)
bq_claims_payment_table_name = "claims_payment"
bq_bronze_claims_payment_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_claims_payment_table_name}"
claims_payment_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/claims_payment.json")

# ==== CLAIMS RESERVES DATA ========
## == source (bucket)
gcs_claims_reserves_source_object = f"{target_env}/claims_reserves/claims_reserves.csv"
gcs_claims_reserves_source_url = f"gs://{gcs_source_data_bucket}/{gcs_claims_reserves_source_object}"
## == bronze layer (bigquery)
bq_claims_reserves_table_name = "claims_reserves"
bq_bronze_claims_reserves_table_id = f"{GCP_PROJECT_ID}.{bq_bronze_dataset}.{bq_claims_reserves_table_name}"
claims_reserves_schema = read_json_schema(f"{PATH_TO_SCHEMAS_SCRIPT}/claims_reserves.json")


import os
from airflow.models import Variable
from include.dbt.cosmos_config import target_env

# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')

# Airflow Variables
settings = Variable.get("insurance_claims_dag_settings", deserialize_json=True)

# DAG Variables
gcs_source_data_bucket = settings['gcs_source_data_bucket']
bq_bronze_dataset = settings['dev_bronze_dataset']
# bq_silver_dataset = settings['dev_silver_dataset']
# bq_gold_dataset = settings['dev_gold_dataset']       

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


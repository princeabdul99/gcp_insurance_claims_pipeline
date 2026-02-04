import os
from cosmos import ProjectConfig, ProfileConfig
from cosmos.profiles.bigquery import GoogleCloudServiceAccountFileProfileMapping


GCP_PROJECT_ID = os.getenv('PROJECT_ID')
BIGQUERY_CONN_ID = os.getenv("BIGQUERY_CONN_ID")
DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME")
KEY_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

BASE_PATH = "/usr/local/airflow/include"
DBT_PROJECT_PATH = f"{BASE_PATH}/dbt"
DBT_PROFILES_PATH = f"{BASE_PATH}/dbt/profiles.yml"


# Environment (dev/prod)
target_env = 'dev'

# Dbt config
DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)
DBT_PROFILE_CONFIG = ProfileConfig(
    profile_name="gcp_insurance_claim",
    target_name=target_env,
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="bigquery_default",
        profile_args={
            "project": GCP_PROJECT_ID,
            "dataset": f"ic_{target_env}",   # dev => ic_dev, prod => ic_prod (ex: ic_dev_bronze, ic_prod_bronze)
            "keyfile": KEY_FILE,
        },
    ),
)



# BIGQUERY_CONN_ID = os.getenv("BIGQUERY_CONN_ID", "bigquery_default")
# DBT_PROJECT_NAME = os.getenv("DBT_PROJECT_NAME", "gcp_insurance_claim")
# BASE_PATH = Path(__file__).resolve().parent.parent
# DBT_PROJECT_PATH = (BASE_PATH / "include" / "dbt").resolve().as_posix()
# DBT_PROFILES_PATH = (BASE_PATH / "include" / "dbt"/ "profiles.yml").as_posix()
# DATASET = os.getenv("DS_DEV", "ic_dev")  # switch per env if needed

# # Dbt config
# _project_config = ProjectConfig(
#     dbt_project_path=DBT_PROJECT_PATH,
# )
# _profile_config = ProfileConfig(
#     profile_name="gcp_insurance_claim",
#     target_name=target_env,
#     profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
#         conn_id=BIGQUERY_CONN_ID,
#     ),
#     # profiles_yml_filepath=DBT_PROFILES_PATH,
# )

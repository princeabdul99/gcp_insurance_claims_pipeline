import os
from dotenv import load_dotenv
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime

load_dotenv()


PATH_TO_DATA_SCRIPT = "/usr/local/airflow/include/ingestion/dev_main.py"

args = {
    'owner': 'abdul-dev'
}

# Environment Variables
GCP_PROJECT_ID = os.getenv('PROJECT_ID')



source_path='https://raw.githubusercontent.com/princeabdul99/practice-dataset/refs/heads/main/insurance_claims'

brokers_data = f'{source_path}/brokers.csv'
coverages_data = f'{source_path}/coverages.csv'


@dag(
    start_date=datetime(2026, 1, 1),
    schedule=None,
    doc_md=__doc__,
    tags=["insurance claims"],
    default_args = args,
)

def insurance_claims_pipeline():
    external_file_to_gcs = BashOperator(
        task_id = "external_file_to_gcs",
        bash_command = f"""
            python {PATH_TO_DATA_SCRIPT} \
            --source_filepath "{{{{ dag_run.conf.get('source_filepath', '{brokers_data}') }}}}"
        """
    )

    external_file_to_gcs_cov = BashOperator(
        task_id = "external_file_to_gcs_cov",
        bash_command = f"""
            python {PATH_TO_DATA_SCRIPT} \
            --source_filepath "{{{{ dag_run.conf.get('source_filepath', '{coverages_data    }') }}}}"
        """
    )

    external_file_to_gcs
    external_file_to_gcs_cov

insurance_claims_pipeline()    

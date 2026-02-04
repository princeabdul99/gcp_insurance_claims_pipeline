from urllib.parse import urlparse
import os
import re

def derive_folder_from_filename(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    name = re.sub(r"_\d{4}-\d{2}-\d{2}$", "", name)
    return name.lower()


def get_filename_from_url(url: str) -> str:
    path = urlparse(url).path
    return os.path.basename(path)


def execute_sql(sql_tbl_path:str):
    with open(sql_tbl_path, "r") as file:
        create_ext_tbl_sql = file.read()

    return create_ext_tbl_sql


PATH_TO_DATA_SCRIPT = "/usr/local/airflow/include/ingestion/bronze_ext_tbl.sql"
dev_bronze_ext_table = execute_sql(f"{PATH_TO_DATA_SCRIPT}")
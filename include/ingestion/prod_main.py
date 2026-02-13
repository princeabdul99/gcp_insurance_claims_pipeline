
import sys
import os
import argparse
from pathlib import Path
import pandas as pd
from io import StringIO
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from time import time
from dotenv import load_dotenv
# from plugins.helper import derive_folder_from_filename
# import plugins.helper as hp

load_dotenv()

import re

def derive_folder_from_filename(filename: str) -> str:
    name = os.path.splitext(filename)[0]
    name = re.sub(r"_\d{4}-\d{2}-\d{2}$", "", name)
    return name.lower()

PROJECT_ID = os.getenv('PROJECT_ID', 'ecom-pipeline-gcp')
BUCKET_NAME = os.getenv('BUCKET_NAME', 'insurance_claims_bucket')


def prod_main(source_filepath: str, base_prefix:str = "prod"):
    
    try:
        if source_filepath.startswith(("http://", "https://")):
            source_path = source_filepath
            filename = source_filepath.split("/")[-1]
        else:
            source_path = Path(source_filepath)
            # if not source_path.exists():
            if not source_path.exists():
                raise FileNotFoundError(f"Source file not found: {source_path}")
            filename = source_path.name
        

        
        folder_name = derive_folder_from_filename(filename)
        gcs_object_path = f"{base_prefix.rstrip('/')}/{folder_name.rstrip('/')}/{filename}"

        # Read CSV into panda and convert datafram back to CSV
        iter_csv = pd.read_csv(source_path, iterator=True, chunksize=10000)
        print(iter_csv)

        # Upload to GCS
        client = storage.Client(project=PROJECT_ID)
        bucket = client.get_bucket(BUCKET_NAME)   
        blob = bucket.blob(gcs_object_path)

        while True:
            try:
                t_start = time()
                csv = next(iter_csv)
                buffer = StringIO()
                csv.to_csv(buffer, index=False)
                buffer.seek(0)
                t_end = time()

                blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

                print(f'Inserted {filename} chunk...Duration: %.3f seconds' %(t_end - t_start))

            except StopIteration:
                print(
                    f"Uploaded {source_filepath} "
                    f"to gs://{BUCKET_NAME}/{gcs_object_path}"
                )
                break  

    except FileNotFoundError as e:
        print(f"FILE ERROR: {e}", file=sys.stderr)
        raise

    except GoogleAPIError as e:
        print(f"GCS API ERROR: {e}", file=sys.stderr)  
        raise

    except Exception as e:
        print(f"UNEXPECTED ERROR: {e}", file=sys.stderr)
        raise 



def main():
    parser = argparse.ArgumentParser(description="Ingest data to data sources")

    parser.add_argument('--source_filepath', help='url of csv/parquet file', required=True)
    args = parser.parse_args()
    prod_main(source_filepath=args.source_filepath)

if __name__ == '__main__':
    main()


# source_filepath='D:/dataset/insurance_claims/claims_announcement.csv'







# import requests
# from google.cloud import storage


# def ingest_data_to_gcs():
#     url =  'https://raw.githubusercontent.com/princeabdul99/practice-dataset/refs/heads/main/insurance_claims/coverages.csv'

#     client = storage.Client()
#     bucket = client.bucket('')
#     blob = bucket.blob()

#     with requests.get(url, stream=True) as r:
#         r.raise_for_status()
#         with blob.open("wb") as f:
#             for chunk in r.iter_content(chunk_size=1024 * 1024):
#                 f.write(chunk)


# if __name__ == '__main__':
#     ingest_data_to_gcs()
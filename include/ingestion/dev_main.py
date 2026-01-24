
import sys
import os
import argparse
from pathlib import Path
import pandas as pd
from io import StringIO
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError

from dotenv import load_dotenv
from plugins.helper import derive_folder_from_filename

load_dotenv()

PROJECT_ID = os.getenv('PROJECT_ID')
BUCKET_NAME = os.getenv('BUCKET_NAME')


def dev_main(source_filepath: str, base_prefix:str = "dev"):

    try:
        source_path = Path(source_filepath)
        if not source_path:
            raise FileNotFoundError(f"Source file not found: {source_path}")

        filename = source_path.name
        folder_name = derive_folder_from_filename(filename)
        gcs_object_path = f"{base_prefix.rstrip('/')}/{folder_name.rstrip('/')}/{filename}"

        # Read CSV into panda and convert datafram back to CSV
        df = pd.read_csv(source_filepath, nrows=100)
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)


        # Upload to GCS
        client = storage.Client(project=PROJECT_ID)
        bucket = client.get_bucket(BUCKET_NAME)   
        blob = bucket.blob(gcs_object_path)

        if blob.exists():
            print(f"Skipping: {gcs_object_path} already exists")
            return "Skipped"

        blob.upload_from_string(buffer.getvalue(), content_type="text/csv", if_generation_match=0)

        print(
            f"Uploaded {source_filepath} "
            f"to gs://{BUCKET_NAME}/{gcs_object_path}"
        )
        return

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
    dev_main(source_filepath=args.source_filepath)    

if __name__ == '__main__':
    main()

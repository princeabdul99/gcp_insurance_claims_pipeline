import requests
from google.cloud import storage


def ingest_data_to_gcs():
    url =  'https://raw.githubusercontent.com/princeabdul99/practice-dataset/refs/heads/main/insurance_claims/coverages.csv'

    client = storage.Client()
    bucket = client.bucket('')
    blob = bucket.blob()

    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with blob.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


if __name__ == '__main__':
    ingest_data_to_gcs()
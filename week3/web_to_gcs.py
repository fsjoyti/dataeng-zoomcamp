import io
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from google.cloud import storage

# Change this to your bucket name
BUCKET_NAME = "test-data-lake-bucket"
from pathlib import Path

CREDENTIALS_FILE = (
    "C:\\Users\\fahmi\\Downloads\\vibrant-mantis-376307-e79d575cbb83.json"
)
CLIENT = storage.Client.from_service_account_json(CREDENTIALS_FILE)

init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
CHUNK_SIZE = 8 * 1024 * 1024

fhv_schema = pa.schema(
    [
        pa.field("dispatching_base_num", pa.string()),
        pa.field("pickup_datetime", pa.timestamp("s")),
        pa.field("dropOff_datetime", pa.timestamp("s")),
        pa.field("PUlocationID", pa.int64()),
        pa.field("DOlocationID", pa.int64()),
        pa.field("SR_Flag", pa.int64()),
        pa.field("Affiliated_base_number", pa.string()),
    ]
)


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    bucket = CLIENT.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.chunk_size = CHUNK_SIZE
    blob.upload_from_filename(local_file)


def download_file(url):
    local_filename = url.split("/")[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    return local_filename


def web_to_gcs(arguments):
    year = arguments[0]
    service = arguments[1]
    print(f"Starting downloading for year: {year} , service {service}")
    for i in range(12):

        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]
        print(month)

        # csv file_name
        upload_year_month(year, service, month)


def upload_year_month(year, service, month):
    file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

    # download it using requests via a pandas df
    request_url = f"{init_url}{service}/{file_name}"
    r = requests.get(request_url)
    open(file_name, "wb").write(r.content)
    print(f"Local: {file_name}")

    # read it back into a parquet file
    df = pd.read_csv(file_name, compression="gzip", low_memory=False)
    file_name = file_name.replace(".csv.gz", ".parquet")
    df.to_parquet(file_name, engine="pyarrow")
    print(f"Parquet: {file_name}")
    table = pq.read_table(file_name)
    cast_table = table.cast(target_schema=fhv_schema)
    destination = f"{Path(file_name).stem}_normalized.parquet"
    pq.write_table(cast_table, destination)

    # upload it to gcs
    upload_to_gcs(BUCKET_NAME, f"{service}/{file_name}", destination)
    print(f"GCS: {service}/{file_name}")


if __name__ == "__main__":
    print("Starting program")
    services = ["fhv"]
    # upload_year_month("2019", "yellow", "06")
    web_to_gcs((2019, "fhv"))
    web_to_gcs_arguments = [(2020, service) for service in services]
    # print(web_to_gcs_arguments)
    # with ThreadPoolExecutor(max_workers=4) as executor:
    #     executor.map(web_to_gcs, web_to_gcs_arguments)

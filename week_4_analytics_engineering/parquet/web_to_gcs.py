import io
import os
import requests
import pandas as pd
import numpy as np
import pyarrow
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get('GCP_GCS_BUCKET', "prefect-de-zoomcamp_tick")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv'
        
        url = init_url + service + '/' + service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # download it using requests via a pandas df

        #https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz
        
        # read it back into a parquet file
        df = pd.read_csv(url, 
                         dtype={'passenger_count': 'Int64', 
                                'payment_type': 'Int64',
                                'RatecodeID' : 'Int64',
                                'trip_type' : 'Int64',
                                'VendorID' : 'Int64',
                                }  
                         )
        # df.to_csv(file_name)
        file_name = file_name.replace('.csv', '.parquet')
        df.to_parquet(f'{file_name}', engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')

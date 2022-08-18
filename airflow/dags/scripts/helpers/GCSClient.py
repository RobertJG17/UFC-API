from dotenv import find_dotenv, load_dotenv
from google.oauth2 import service_account
# Imports the Google Cloud client library
from google.cloud import storage
from io import BytesIO
import pandas as pd
import json
import os


class GCSClient:
    SERVICE_ACCT_CREDENTIALS = None
    bucket_name = None

    def __init__(self, blob_path):
        self.blob_path = blob_path 
        if self.bucket_name is None:
            load_dotenv(find_dotenv())
            self.bucket_name = os.environ.get('UFC_BUCKET')
            creds = json.loads(os.environ.get('SERVICE_ACCT_JSON'))
            self.SERVICE_ACCT_CREDENTIALS = service_account.Credentials.from_service_account_info(creds) 

    def upload_to_bucket(self, contents):
        bucket = storage.Client(credentials=self.SERVICE_ACCT_CREDENTIALS).bucket(self.bucket_name)
        blob = bucket.blob(self.blob_path)
        blob.upload_from_string(contents)
        print('FILE UPLOADED SUCCESSFULLY')
     
    def fetch_from_bucket(self):
        bucket = storage.Client(credentials=self.SERVICE_ACCT_CREDENTIALS).bucket(self.bucket_name)
        blob = bucket.blob(self.blob_path)
        print('FILE FETCHED SUCCESSFULLY')
        return blob

    def create_df_from_blob(self, blob):
        f = BytesIO()
        blob.download_to_file(f)
        df = pd.read_parquet(f)
        print('DF CREATED SUCCESSFULLY')
        return df
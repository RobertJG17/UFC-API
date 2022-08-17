from google.oauth2 import service_account
from os.path import join, dirname
# Imports the Google Cloud client library
from google.cloud import storage
from dotenv import load_dotenv
from io import BytesIO
import pandas as pd
import os


class GCSClient:
    SERVICE_ACCT_CREDENTIALS = service_account.Credentials.from_service_account_file('../service_acct.json')
    bucket_name = None

    def __init__(self, blob_path):
        self.blob_path = blob_path 
        if self.bucket_name is None:
            dotenv_path = join(dirname('../__file__'), '.env')
            load_dotenv(dotenv_path)
            self.bucket_name = os.environ.get('UFC_BUCKET')

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
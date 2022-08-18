from google.cloud.sql.connector import Connector
from dotenv import load_dotenv, find_dotenv
from google.oauth2 import service_account
import pg8000
import json
import os


# LOAD .ENV TO ACCESS SENSITIVE DATA
load_dotenv(find_dotenv())

# CONSTANTS, NEED TO ENCRYPT/CREATE AUTHENTICATION LAYER
pg_driver = os.environ.get('PG_DRIVER')
db_user = os.environ.get('CLOUD_SQL_USER_NAME')
db_pass = os.environ.get('CLOUD_SQL_DB_PASSWORD')
db_name = os.environ.get('CLOUD_SQL_DATABASE_NAME')
db_port = os.environ.get('PORT')
cloud_sql_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME')
service_acct_json = json.loads(os.environ.get('SERVICE_ACCT_JSON'))

SERVICE_ACCT_CREDENTIALS = service_account.Credentials.from_service_account_info(service_acct_json)

def getconn():
    with Connector(enable_iam_auth=True, credentials=SERVICE_ACCT_CREDENTIALS) as connector:
        conn = connector.connect(
            cloud_sql_connection_name,
            pg_driver,
            user=db_user,
            password=db_pass,
            db=db_name,
        )

    return conn
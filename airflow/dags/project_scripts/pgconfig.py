from google.cloud.sql.connector import Connector
from google.oauth2 import service_account
from os.path import join, dirname
from dotenv import load_dotenv
import pg8000
import os


# LOAD .ENV TO ACCESS SENSITIVE DATA
dotenv_path = join(dirname('../__file__'), '.env')
load_dotenv(dotenv_path)

# CONSTANTS, NEED TO ENCRYPT/CREATE AUTHENTICATION LAYER
pg_driver = os.environ.get('PG_DRIVER')
db_user = os.environ.get('CLOUD_SQL_USER_NAME')
db_pass = os.environ.get('CLOUD_SQL_DB_PASSWORD')
db_name = os.environ.get('CLOUD_SQL_DATABASE_NAME')
db_port = os.environ.get('PORT')
cloud_sql_connection_name = os.environ.get('CLOUD_SQL_CONNECTION_NAME')

SERVICE_ACCT_CREDENTIALS = service_account.Credentials.from_service_account_file('../service_acct.json')

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
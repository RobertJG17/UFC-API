from dotenv import load_dotenv, find_dotenv
from helpers.GCSClient import GCSClient
from helpers.pgconfig import getconn
import sqlalchemy
import os

# LOAD ENV FILE
load_dotenv(find_dotenv())

# SCRIPT FOR DATA LOAD | UPDATES
curated_blob_path = os.environ.get('MERGE_GCS_PATH')
gcs_curated_client = GCSClient(blob_path=curated_blob_path)
blob = gcs_curated_client.fetch_from_bucket()
df = gcs_curated_client.create_df_from_blob(blob)

conn = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

with conn.begin() as init_conn:
    try:
        df.to_sql(name='fighters', con=init_conn, if_exists='append', index=False, chunksize=100)
        print('FILE LOADED INTO SQL')
    except Exception as e:
        print(e)


# engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=getconn)
# with engine.connect() as db_conn:
#     query = db_conn.execute('''SELECT COUNT(*) FROM fighters;''')

# print(query.fetchall())



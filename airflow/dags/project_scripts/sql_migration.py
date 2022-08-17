from GCSClient import GCSClient
from pgconfig import getconn
import sqlalchemy

# SCRIPT FOR DATA LOAD | UPDATES

gcs_client = GCSClient()
blob = gcs_client.fetch_from_bucket('curated/curated.parquet')
df = gcs_client.create_df_from_blob(blob)

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



from multiprocessing.sharedctypes import Value
from dotenv import load_dotenv, find_dotenv
from utils.GCSClient import GCSClient
from time import time
import pandas as pd, numpy as np
import os


def clean(df: pd.DataFrame):
    # STANDARDIZING COLUMNS
    old_cols = df.columns
    standarized_cols = [
        col.lower().replace(' ', '-').replace('.', '').replace('/', '-')
        for col in df.columns
    ]
    new_col_dict = {old_cols[idx]: standarized_cols[idx] for idx in range(len(df.columns))}

    # APPENDING PM TO METRICS MEASURED PER MINUTE
    df = df.rename(new_col_dict, axis=1)
    df = df.rename({
        'sig-str-landed': 'sig-str-landed-pm', 
        'sig-str-absorbed': 'sig-str-absorbed-pm'}, 
    axis=1)
    
    # DYNAMIC CAST USING NESTED TRY EXCEPT BLOCKS
    for col in df.columns:
        try:
            df.loc[:, col] = df.loc[:, col].replace('', -1).replace(np.NaN, -1)
            if df.loc[:, col].apply(lambda val: int(val)) != df.loc[:, col]:
                raise ValueError
            df.loc[:, col] = df.loc[:, col].astype('int')
        except ValueError:
            try:
                df.loc[:, col] = df.loc[:, col].replace(-1, '')
                df.loc[:, col] = df.loc[:, col].replace('', -1.0)
                df.loc[:, col] = df.loc[:, col].astype('float')
            except ValueError:
                df.loc[:, col] = df.loc[:, col].replace(-1.0, '').replace(-1, '')
                df.loc[:, col] = df.loc[:, col].replace('', '-')
                df.loc[:, col] = df.loc[:, col].astype('str')


    # FILLING NA VALUES IN OBJECT OR STR COLUMNS WITH EMPTY STR
    cols = ['wins', 'losses', 'draws']
    for idx in range(len(cols)):
        df.loc[:, cols[idx]] = df['record'].apply(lambda rec: rec.split(' ')[0].split('-')[idx])
        df.loc[:, cols[idx]] = df.loc[:, cols[idx]].astype(int)

    return df[sorted(df.columns)]



# ENTRY POINT FOR DAG REFERENCE
def data_merge_entrypoint():
    # LOAD .ENV TO ACCESS SENSITIVE DATA
    load_dotenv(find_dotenv())
    
    start = time()

    # FETCH FIGHTERS.PARQUET AND LOAD INTO DF
    fighters_blob_path = os.environ.get('FIGHTERS_GCS_PATH')
    gcs_fighters_client = GCSClient(blob_path=fighters_blob_path)
    fighters_blob = gcs_fighters_client.fetch_from_bucket()
    fighters_df = gcs_fighters_client.create_df_from_blob(fighters_blob)

    # FETCH STATS.PARQUET FILE AND LOAD INTO DF
    stats_blob_path = os.environ.get('STATS_GCS_PATH')
    gcs_stats_client = GCSClient(blob_path=stats_blob_path)
    stats_blob = gcs_stats_client.fetch_from_bucket()
    stats_df = gcs_stats_client.create_df_from_blob(stats_blob)

    # MERGE DF | CLEAN DF | WRITE TO PARQUET FILE
    merged = fighters_df.merge(stats_df, on="slug", )
    curated = clean(merged)
    curated_file = curated.to_parquet()

    # UPLOAD CLEANED FILE TO GCS BUCKET
    curated_blob_path = os.environ.get('MERGE_GCS_PATH')
    gcs_curated_client = GCSClient(blob_path=curated_blob_path)
    gcs_curated_client.upload_to_bucket(curated_file)

    end = time()
    print(f"Script took {end - start} seconds to complete")


if __name__ == "__main__":
    # LOAD .ENV TO ACCESS SENSITIVE DATA
    load_dotenv(find_dotenv())

    # RUN SCRIPT
    data_merge_entrypoint()

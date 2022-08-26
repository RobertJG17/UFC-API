from curses import keyname
from dotenv import load_dotenv, find_dotenv
from utils.GCSClient import GCSClient
from time import time
import pandas as pd, numpy as np
import os, re


def clean(df: pd.DataFrame):
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
    
    entry = df.loc[df["fighter"] == "Charles Oliveira"]
    entry = df.iloc[entry.index].reset_index(drop=True).squeeze().to_dict()
    
    # Iterate thru columns
    modified_col_dtype = {}
    for key, val in entry.items():
        # check if val contains alphabetical characters with regexp
        str_regexp = r'[a-zA-Z]|\s|-|:|/'
        # str_chars = [' ', '/', '-', ':']
        # or any(char in str_val for char in str_chars)
        str_val = str(val)

        if re.search(str_regexp, str_val):
            df.loc[:, key] = df.loc[:, key].replace('', '-').replace(np.NaN, '-')
            modified_col_dtype[key] = 'string'
        elif '.' in str_val:
            df.loc[:, key] = df.loc[:, key].replace('', -1).replace(np.NaN, -1)
            modified_col_dtype[key] = 'float'
        else:
            df.loc[:, key] = df.loc[:, key].replace('', -1).replace(np.NaN, -1)
            modified_col_dtype[key] = 'int'

    df = df.astype(modified_col_dtype)


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
    merged = fighters_df.merge(stats_df, left_on="name", right_on="Fighter")
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

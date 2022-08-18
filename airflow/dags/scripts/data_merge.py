from dotenv import load_dotenv, find_dotenv
from helpers.GCSClient import GCSClient
from time import time
import pandas as pd
import os


def clean(df: pd.DataFrame):
    col_mapping = {
        'Sig. Str. Landed': 'ss-landed-pm', 'Sig. Str. Absorbed': 'ss-absorbed-pm',
        'Takedown avg': 'tkd-avg', 'Submission avg': 'sub-avg', 'Sig. Str. Defense': 'ss-def', 'Takedown Defense': 'tkd-def',
        'Knockdown Ratio': 'knockdown-ratio', 'Height': 'height', 'Weight': 'weight', 'Reach': 'reach', 'Leg reach': 'leg-reach', 
        'Title Defenses': 'title-defenses', 'Fight Win Streak': 'fight-win-streak', 'Sig. Strikes Attempted': 'ss-attempted', 
        'Sig. Strikes Landed': 'ss-landed', 'Standing': 'standing', 'Clinch' : 'clinch', 'Ground': 'ground', 'KO/TKO': 'ko-tko', 
        'DEC': 'dec', 'SUB': 'sub', 'Age': 'age', 'Takedowns Landed': 'tkd-landed', 'Takedowns Attempted': 'tkd-attempted', 
        'img': 'href', 'nickName': 'nick-name', 'weightClass': 'weight-class', 'Fighter': 'fighter', 'Former Champion': 'former-champion',
        'Average fight time': 'avg-fight-time', 'Status': 'status', 'Hometown': 'hometown', 'Octagon Debut': 'oct-debut',
        'Trains at': 'trains-at', 'Fighting style': 'style', 'First Round Finishes': 'first-round-finishes'
    }

    df = df.rename(col_mapping, axis=1)
    df = df.drop(['Wins by Decision', 'Wins by Knockout', 'Wins by Submission'], axis=1)

    # CHANGING 
    # Sig. Strikes Attempted, Sig. Str. Landed, Sig. Str. Absorbed, Standing, Clinch, Ground, KO/TKO, DEC, SUB 
    # TO INT
    int_cols = [
    "age", "fight-win-streak", "ss-landed", "ss-attempted", 
    "standing", "clinch", "ground", "ko-tko", "dec", "sub",
    "first-round-finishes"
    ]
    df.loc[:, int_cols] = df.loc[:, int_cols].fillna(0)
    df.loc[:, int_cols] = df.loc[:, int_cols].astype(int)

    float_cols = [
    "height", "knockdown-ratio", "leg-reach", "reach", "ss-absorbed-pm", 
    "ss-def", "ss-landed-pm", "sub-avg", "title-defenses",	"tkd-attempted", 
    "tkd-avg",	"tkd-def", "tkd-landed"
    ]
    df.loc[:, float_cols] = df.loc[:, float_cols].fillna(0.0).replace('', 0.0)
    df.loc[:, float_cols] = df.loc[:, float_cols].astype(float)

    # FILLING NA VALUES IN OBJECT OR STR COLUMNS WITH EMPTY STR
    cols = ['wins', 'losses', 'draws']
    for idx in range(len(cols)):
        df.loc[:, cols[idx]] = df['record'].apply(lambda rec: rec.split(' ')[0].split('-')[idx])
        df.loc[:, cols[idx]] = df.loc[:, cols[idx]].astype(int)

    return df[sorted(df.columns)]



# ENTRY POINT FOR DAG REFERENCE
def data_merge_entrypoint():
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

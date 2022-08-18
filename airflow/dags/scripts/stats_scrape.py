from helpers.parsers.stats_parser_funcs import parse_soup
from aiohttp import ClientSession, TCPConnector
from dotenv import load_dotenv, find_dotenv
from helpers.GCSClient import GCSClient
from bs4 import BeautifulSoup
from time import time
import pandas as pd
import asyncio
import os


# DRIVER CODE FOR ASYNCHRONOUS REQUESTS
async def fetch(url, session, fighter, slug):
     async with session.get(url) as response:
          text = await response.text()
          soup = BeautifulSoup(text, 'html.parser')
          info = await get_fighter_data(soup, fighter, slug)
          return info

async def fetch_with_sem(url, session, sem, fighter, slug):
     async with sem:
          return await fetch(url, session, fighter, slug)

async def get_fighter_data(soup, fighter, slug):
     fighter_obj = parse_soup(soup, fighter, slug)
     return fighter_obj

async def main():
     tasks = []
     sem = asyncio.Semaphore(100)
     base_url = "https://www.ufc.com/athlete/"

     # FETCH FIGHTERS.PARQUET FILE
     fighters_blob_path = os.environ.get('FIGHTERS_GCS_PATH')
     gcs_fighters_client = GCSClient(blob_path=fighters_blob_path)
     fighters_blob = gcs_fighters_client.fetch_from_bucket()
     fighters_df = gcs_fighters_client.create_df_from_blob(fighters_blob)
     fighters_df['slug'] = fighters_df['name'].apply(lambda n: n.lower().replace(' ','-'))

     async with ClientSession(connector=TCPConnector(ssl=False)) as session:
          for idx in fighters_df.index:
               print(fighters_df.loc[idx, 'name'])
               tasks.append(
                    fetch_with_sem(
                         base_url + fighters_df.loc[idx, "slug"], 
                         session, 
                         sem,
                         fighters_df.iloc[idx]["name"],
                         fighters_df.iloc[idx]["slug"]
                    )
               )

          records = await asyncio.gather(*tasks)
          return records


          
# ENTRY POINT FOR DAG REFERENCE
def stats_entrypoint():
     start = time()
     records = asyncio.run(main())

     df = pd.DataFrame.from_records(records)
     parquet_file = df.to_parquet(engine='pyarrow')

     # method call
     stats_blob_path = os.environ.get('STATS_GCS_PATH')
     gcs_stats_client = GCSClient(blob_path=stats_blob_path)
     gcs_stats_client.upload_to_bucket(parquet_file)

     end = time()
     print('Script took {} seconds to complete'.format(end-start))
    

if __name__ == "__main__":
     # LOAD .ENV TO ACCESS SENSITIVE DATA
     load_dotenv(find_dotenv())

     # RUN SCRIPT
     stats_entrypoint()
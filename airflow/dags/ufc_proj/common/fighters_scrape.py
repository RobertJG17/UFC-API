from utils.parsers import extract_info
from aiohttp import ClientSession, TCPConnector
from dotenv import load_dotenv, find_dotenv
from utils.GCSClient import GCSClient
from bs4 import BeautifulSoup
from itertools import chain
from time import time
import pandas as pd
import asyncio
import os


# DRIVER CODE FOR ASYNCHRONOUS REQUESTS
async def fetch(url, session):
    async with session.get(url) as response:
        text = await response.text()
        soup = BeautifulSoup(text, 'html.parser')
        info = await get_fighter_cards(soup)
        return info

async def fetch_with_sem(url, session, sem):
    async with sem:
        return await fetch(url, session)

async def get_fighter_cards(soup):
    fighter_info = []
    fighter_cards = soup.find_all('div', attrs={'class': 'c-listing-athlete-flipcard__front'})
    for fighter in fighter_cards:
        fighter_info.append(extract_info(fighter))
    return fighter_info

async def main():
    tasks = []
    sem = asyncio.Semaphore(10)

    async with ClientSession(connector=TCPConnector(ssl=False)) as session:

        # HACKY WAY TO GET NUMBER OF PAGES ROUGHLY NEEDED TO SEND REQUESTS TO,
        # SCRAPING ATHLETE COUNT ELEMENTS THEN DIVIDING BY THE NUMBER OF ATHLETES SHOWN ON EACH PAGE
        async with session.get('https://www.ufc.com/athletes/all?gender=All&page=0') as response:
            text = await response.text()
            soup = BeautifulSoup(text, 'html.parser')
            numOfFighters = soup.find('div', attrs={'class': "althelete-total"}).text.split(' ')[0]
            MAX_PAGES = (int(numOfFighters) // 11) + 1

        for page in range(MAX_PAGES):
            url = f'https://www.ufc.com/athletes/all?gender=All&page={page}'
            tasks.append(
                fetch_with_sem(url, session, sem)
            )
        
        records = await asyncio.gather(*tasks)
        return records   


# HELPER METHOD TO HANDLE DUPES AND ADD NEW COL
def modify(df: pd.DataFrame):
    df['slug'] = df['name'].apply(lambda n: n.lower().replace(' ','-'))
    dup_idx = df[(df['slug'] == 'bruno-silva') & (df['nick-name'] == 'Blindado')].index
    df.loc[dup_idx, 'slug'] = df.loc[dup_idx, 'slug'] + '-' + df.loc[dup_idx, 'nick-name']
    df.loc[dup_idx, 'slug'] = df.loc[dup_idx, 'slug'].apply(lambda s: s.lower())
    df = df.drop_duplicates(subset='slug', keep='last', ignore_index=True)
    return df


# ENTRY POINT FOR DAG REFERENCE
def fighters_entrypoint():
    # LOAD .ENV TO ACCESS SENSITIVE DATA
    load_dotenv(find_dotenv())

    start = time()
    pages = asyncio.run(main())
    # converting 2d list into 1d
    # using chain.from_iterables
    # snippet pulled from: https://www.geeksforgeeks.org/python-ways-to-flatten-a-2d-list/
    records = list(chain.from_iterable(pages))
    df = pd.DataFrame.from_records(records)
    df = modify(df)
    parquet_file = df.to_parquet(engine='pyarrow')
    
    # method call
    fighters_blob_path = os.environ.get('FIGHTERS_GCS_PATH')
    gcs_fighter_client = GCSClient(blob_path=fighters_blob_path)
    gcs_fighter_client.upload_to_bucket(parquet_file)

    end = time()
    print('Script took {} seconds to complete'.format(end-start))


if __name__ == "__main__":
    # LOAD .ENV TO ACCESS SENSITIVE DATA
    load_dotenv(find_dotenv())

    # RUN SCRIPT
    fighters_entrypoint()

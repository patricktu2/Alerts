import requests
import os
import argparse
import pandas as pd
import time 
from time import sleep
import traceback
from dotenv import load_dotenv
import warnings
from random import randint

warnings.filterwarnings("ignore")

global COLLECTION
COLLECTION = "jankyheist" # "cryptopunks", "boredapeyachtclub"
OUTPUT_PATH = f'../Data/{COLLECTION}_newest_listings.pkl'

load_dotenv()
OPENSEA_APIKEY = str(os.getenv('OPENSEA_APIKEY'))    
SLEEP = 10


def write_dummy_file():
    # dummpy .py file to import in streamlit app, so that it refreshes the app
    filePath = '../dummy.py'
    with open(filePath, 'w') as f:
        f.write(f'# Dummy file to be overwritten by scrape_new_listings.py and imported in streamlit app dashboard.py \n')
        f.write(f'# so that it detects new changes in source code and refreshes the app with new data \n')
        f.write(f'# Random Number: {randint(0, 10000)}')

def get_events(collection_slug, limit=300, offset=0, json_file="", event_type=''):
    '''
    Perform HTTP Get Request to get current events on open sea for a given collection
    '''
    url = "https://api.opensea.io/api/v1/events"
    querystring = {
        "collection_slug":collection_slug,
        "only_opensea":"false", 
        "offset":"{}".format(offset), 
        "limit":"{}".format(limit), 
                  }
    
    if event_type != '':
        querystring['event_type'] = event_type

    apikey = OPENSEA_APIKEY
    headers = None if apikey == "" else {"X-API-KEY": apikey}
    response = requests.request("GET", url, headers=headers, params=querystring)

    if "<!doctype html>" in response.text[:20].lower():
        return None # blocked request
    return response.json()
    
def get_new_listings(collection, limit=30):
    '''
    Get the newest listings for a collection as a pd.Dataframe for a collection
    '''
    ETH_NORM_CONSTANT = 1000000000000000000
    # get data via opensea api
    opensea_response = get_events(collection, limit=limit, event_type="created")
    #cleaned_response = clean_response(opensea_response)
    # turn json in dataframe
    df_new_list = pd.DataFrame(opensea_response["asset_events"])
    # some parsing and cleaning
    df_new_list["token_id"] = df_new_list['asset'].apply(pd.Series)["token_id"].astype(int)
    df_new_list["payment_token_symbol"] = df_new_list["payment_token"].apply(lambda x: x["symbol"])
    cols = [ "token_id","event_type", "created_date", "starting_price", "ending_price", "payment_token_symbol"]
    df_rec_listings = df_new_list[cols]
    df_rec_listings["starting_price"] = df_rec_listings["starting_price"].astype(float)/ETH_NORM_CONSTANT
    df_rec_listings["ending_price"] = df_rec_listings["ending_price"].astype(float)/ETH_NORM_CONSTANT
    df_rec_listings["token_id"] = df_rec_listings["token_id"].astype(int)
    
    df_rec_listings['created_date'] = pd.to_datetime(df_rec_listings['created_date'], format='%Y-%m-%d %H:%M')
#    df_rec_listings['listing_date'] = pd.to_datetime(df_rec_listings['listing_date'])
    
    return df_rec_listings


def main(collection=COLLECTION, time_intervall=SLEEP):
    while True:
        try:
            print(time.strftime('%X %x %Z'), ": Fetch newest listing events on OS for", COLLECTION)
            df_rec_listings = get_new_listings(collection)
            df_rec_listings.to_pickle(OUTPUT_PATH)
            write_dummy_file()
            sleep(time_intervall)
        except Exception:
            print(traceback.print_exc())
            print('Restarting...')
            sleep(time_intervall)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', type=str, help='slug name of collection', default="")
    args = parser.parse_args()

    collection = args.collection
    if collection:
        COLLECTION = collection
        OUTPUT_PATH = f'../Data/{COLLECTION}_newest_listings.pkl'

    main(COLLECTION, SLEEP)

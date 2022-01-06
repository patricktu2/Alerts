import requests
import json
import numpy as np
import pandas as pd
from time import time
import argparse
from joblib import Parallel, delayed
from Opensea_Scrape.preprocess import run_data_preprocessing
import os 

global COLLECTION
COLLECTION = "clonex" # "cryptopunks", "boredapeyachtclub"
OUTPUT_PATH = f'../Data/{COLLECTION}.json'
OPENSEA_APIKEY = str(os.getenv('OPENSEA_APIKEY'))    

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

def get_assets(owner:str=None, token_ids:list=None, limit:int=50, order_by:str = None,
               order_direction:str="asc", offset:int=0, collection:str=None):
    '''
    Performs HTTP GET request to open sea APi to retrieve assets. Returns HTTP response as dict.
    '''
    url = "https://api.opensea.io/api/v1/assets"
    
    # parsing of query parameters
    querystring = dict()
    if owner:
        querystring["owner"]=owner
    if token_ids:
        querystring["token_ids"]=token_ids
    if collection:
        querystring["collection"]=collection
    if order_by:
        querystring["order_by"]=order_by
        
    querystring["limit"]=limit
    querystring["order_direction"]=order_direction
    querystring["offset"]=offset
    
    # perfrom HTTP get request to opensea API
    response = requests.request("GET", url, params=querystring)

    status_code = response.status_code
    
    if status_code==200:
        response_dict = json.loads(response.text)
        no_assets = len(response_dict["assets"])
        #print(f"{no_assets} assets succesfully retrieved")
        return response_dict
        
    else:
        print(f"Error {status_code} occured")
        print(response.text)
        return {}


def get_stats(collection:str=None):
    '''
    Performs HTTP GET request to open sea APi to get statistics for a collection. Returns HTTP response as dict.
    '''
    url = f"https://api.opensea.io/api/v1/collection/{collection}/stats"
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers)
    
    status_code = response.status_code
    
    if status_code==200:
        response_dict = json.loads(response.text)
        #print(f"{no_assets} assets succesfully retrieved")
        return response_dict["stats"]
        
    else:
        print(f"Error {status_code} occured")
        print(response.text)
        return {}


def retrieve_asset_and_unpack(token_ids:list, collection:str, limit:int):
    print(f"{token_ids[0]} - {token_ids[-1]} - {collection}")
    response = get_assets(token_ids=token_ids, collection=collection, limit=limit)
    assets = response["assets"]
    return assets

def run_retrieve_assets(collection:str, n_jobs:int=6):
    '''
    Loops through Opensea API Asset API until all assets from the specified collections are retrieved.
    Collection name needs to be specified as the unique collection slug (e.g. cryptopunks or )
    '''    
    stat_dict = get_stats(collection)
    asset_count = int(stat_dict["count"]) # total assets count in the collection from open sea
    num_ids = 30 # max 30 token_ids as parameter allowed
    start_token_id = 0
    num_retrieved_assets = 0 # counter
    iteration = 1
    no_assets_per_requests = 30 # limit of # arguments for token ids
    max_iterations = int(np.ceil(asset_count/no_assets_per_requests))
    
    
    token_ids_lists = []
    for i in range(0, max_iterations):
        start_token_id = i*30+1
        end_token_id = start_token_id + no_assets_per_requests
        token_ids = list(np.arange(start_token_id, end_token_id))
        token_ids_lists.append(token_ids)

    start_timer = time()
    asset_list = [] # save responses in list
    asset_list = Parallel(n_jobs=n_jobs, verbose=10)(delayed(retrieve_asset_and_unpack)\
        (token_ids=i, collection=collection, limit=no_assets_per_requests) for i in token_ids_lists)
    end_timer = time()
    run_time = end_timer - start_timer
    print(run_time)

    flat_asset_list = [item for sublist in asset_list for item in sublist]
    
    num_retrieved_assets = len(flat_asset_list)
    print(
        f"Retrieved {num_retrieved_assets} assets from {asset_count} in {round(run_time,2)} s. Saved in {OUTPUT_PATH}")
    return flat_asset_list


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', type=str, help='slug name of collection', default="")
    parser.add_argument('--njobs', type=int, help='Number of threads for API requests', default=None)
    args = parser.parse_args()

    collection = args.collection
    if collection:
        COLLECTION = collection
        OUTPUT_PATH = f'../Data/{COLLECTION}.json'


    N_JOBS = 5
    njobs = args.njobs
    if njobs:
        N_JOBS = njobs

    print(f"START Data Ingestion for: {COLLECTION}")
    asset_list = run_retrieve_assets(COLLECTION, N_JOBS)
    # save output
    with open(OUTPUT_PATH, 'w') as fp:
        json.dump(asset_list, fp)


    print(f"START Data Preprocessing for: {COLLECTION}")
    start_timer = time()
    df = pd.DataFrame(asset_list)
    df = run_data_preprocessing(df)
    
    end_timer = time()
    run_time = end_timer-start_timer
    # save data as csv
    OUTPUT_PATH_PROCESSED = f'../Data/{COLLECTION}_processed.pkl'

    df.to_pickle(OUTPUT_PATH_PROCESSED)
    print(f"Data Preprocessing run in {round(run_time, 2)}s and saved in: {OUTPUT_PATH_PROCESSED}")

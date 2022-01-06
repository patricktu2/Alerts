import pandas as pd
import numpy as np
import argparse
import json
from time import time

COLLECTION = "clonex"
INPUT_PATH = f'data/{COLLECTION}.json'
OUTPUT_PATH = f'data/{COLLECTION}_processed.csv'
COLLECTION_SIZE = 0
TRAITS = []
TRAIT_LABELS = []
TRAIT_LABEL_DICT = {}
TRAIT_COUNT_STAT = {}

ETH_NORMALIZATION_CONSTANT = 1000000000000000000


def get_all_trait_types(trait_list):
    '''iterates through trait column and returns unque set of trait types (e.g. type, dna, etcs)'''
    trait_types = set()
    for row in trait_list:
        for trait_dic in row:
            trait_type = trait_dic["trait_type"]
            trait_types.add(trait_type)
    
    return trait_types

def get_trait_column(trait_list):
    ''' Returns a vector / pd.Series of all trait types with corresponding values''' 
    trait_col = {trait: np.nan for trait in TRAIT_LABELS}
    
    rarity_score = 0
    
    for trait_dic in trait_list:
        trait_type = trait_dic["trait_type"]
        trait_value = trait_dic["value"]
        trait_count = trait_dic["trait_count"]
        
        # compute rarity score
        if trait_count == 0: # weird behavior from OPensea API Response
            trait_rarity = 0
        else:
            trait_rarity = 1 / (trait_count/COLLECTION_SIZE)

        rarity_score += trait_rarity
        
        if trait_type not in TRAIT_COUNT_STAT.keys():
            TRAIT_COUNT_STAT[trait_type] = {}
        
        TRAIT_COUNT_STAT[trait_type][trait_value] = trait_count
        

        trait_label = TRAIT_LABEL_DICT[trait_type]
        trait_col[trait_label] = trait_value
    
    trait_col["rarity_score"] = rarity_score
    
    return pd.Series(trait_col)

def make_clickable(val):
    return '<a href="{}">{}</a>'.format(val,val)


def process_trait_data(df):
    '''
    Takes traits column from df and adds a separate trait column per trait with the respective value
    and calculates overall rarity score per item. Potential issues if 1 to n mappings of attributes 
    (e.g. 1 asset kan have multiple values of trait value type)
    '''
    global COLLECTION_SIZE
    COLLECTION_SIZE = df.shape[0]

    # add 1 empty column per trait
    TRAITS = list(get_all_trait_types(df["traits"]))
    for trait in TRAITS:
        trait_ = trait.replace(" ","_")
        trait_label = "Trait_" + trait_
        #df[trait_label] = np.nan
        TRAIT_LABELS.append(trait_label)
        
    # mapping diczionary from open sea to column label of the df
    global TRAIT_LABEL_DICT
    TRAIT_LABEL_DICT = dict(zip(TRAITS, TRAIT_LABELS))

    df_traits = df.apply(lambda x: get_trait_column(x["traits"]), axis=1)
    df = pd.concat([df, df_traits], axis=1)
    df["rarity_rank"] = df["rarity_score"].rank(method="dense", ascending=False).astype(int)
    
    df["permalink"]=df.apply(lambda x: make_clickable(x["permalink"]), axis=1)
    return df


def process_sell_orders(df):
    '''
    Turns sell orders that come as a dictionary from the opensea api into separate columns and 
    parses data types
    '''
    # filter for the ones that are for sale and add to df
    df_for_sale = df[df["sell_orders"].notna()]
    df_sell_orders = df_for_sale["sell_orders"].explode().apply(pd.Series)
    df_sell_orders["current_price"] = df_sell_orders["current_price"].astype(float)/ETH_NORMALIZATION_CONSTANT
    df_sell_orders["payment_token"] = df_sell_orders["payment_token_contract"].apply(lambda x: x['symbol'])
    df_sell_orders["created_date"] = df_sell_orders["created_date"].astype('datetime64[ns]')
    df_sell_orders["closing_date"] = df_sell_orders["closing_date"].astype('datetime64[ns]')
    df = pd.merge(df, df_sell_orders, how="left", left_index=True, right_index=True)

    return df

def process_last_sales(df):
    '''
    Turns last sales of an assets that come as a dictionary from the opensea api into separate columns and 
    parses data types
    '''
    # filter for the ones with a last sale and add to whole df
    df_last_sale = df[df["last_sale"].notna()]
    df_sales = df_last_sale["last_sale"].apply(pd.Series)
    df_sales["event_timestamp"] = df_sales["event_timestamp"].astype('datetime64[ns]')
    df_sales["created_date"] = df_sales["created_date"].astype('datetime64[ns]')
    df_sales["payment_token"] = df_sales["payment_token"].apply(lambda x: x['symbol'])
    df_sales["total_price"] = df_sales["total_price"].astype(float)
    df_sales["quantity"] = df_sales["quantity"].astype(float)
    df_sales["last_sale_price"] = df_sales["total_price"]/(df_sales["quantity"] * ETH_NORMALIZATION_CONSTANT)
    df = pd.merge(df, df_sales, how="left", left_index=True, right_index=True, suffixes=('_sell_order', '_last_sale'))

    return df

def run_data_preprocessing(df):

    df = process_trait_data(df)
    df = process_sell_orders(df)
    df = process_last_sales(df)

    return df


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', type=str, help='slug name of collection', default="")
    args = parser.parse_args()
    collection = args.collection

    if collection:
        COLLECTION = collection
        INPUT_PATH = f'data/{COLLECTION}.json'
        OUTPUT_PATH = f'data/{COLLECTION}_processed.csv'

    # read scraped data from json file and convert to data frame
    with open(INPUT_PATH, 'r') as fp:
        asset_list = json.load(fp)

    print(f"START Data Preprocessing for: {COLLECTION}")
    start_timer = time()
    df = pd.DataFrame(asset_list)
    df = run_data_preprocessing(df)
    
    end_timer = time()
    run_time = end_timer-start_timer

    # save data as csv
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Data Preprocessing run in {round(run_time, 2)}s and saved in: {OUTPUT_PATH}")

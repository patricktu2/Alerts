"""
Streamlit Dashboard that reads local data files in /Data and displays as webapp
"""


import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime
import plotly.express as px
from Opensea_Scrape.scrape_collection import get_new_listings
import os
from dummy import * # workaround for autorefresh, file gest overwritten and app should refresh

# Add a selectbox to the sidebar:
COLLECTION = st.sidebar.selectbox(
    'Choose your collection',
    ('jankyheist', 'clonex', 'huxley')
)

IMPUT_PATH_COLLECTION = f'Data/{COLLECTION}_processed.pkl'
df = pd.read_pickle(IMPUT_PATH_COLLECTION)
df["token_id"] = df["token_id"].astype(int)
df_fixed_price_listings = df[df["payment_token_sell_order"]=="ETH"]
COLLECTION_DEFAULT_TRAIT = {
    "jankyheist":"Trait_Jankyness_Level",
    "clonex": "Trait_DNA"
}

# Recent Listings Snipe Table
IMPUT_PATH_REC_LISTINGS = f'Data/{COLLECTION}_newest_listings.pkl'
df_rec_listings = pd.read_pickle(IMPUT_PATH_REC_LISTINGS)
timestamp_recent_listings = os.path.getctime(IMPUT_PATH_REC_LISTINGS)

TRAIT_OPTIONS = list(df.columns[df.columns.str.contains("Trait")]) # get extracted traits

DEFAULT_TRAIT = TRAIT_OPTIONS.index(COLLECTION_DEFAULT_TRAIT[COLLECTION])

TRAIT = st.sidebar.selectbox(
    'Choose the trait to segment',
    TRAIT_OPTIONS, DEFAULT_TRAIT
)

TRAIT_OPTION_VALUES = [None]
TRAIT_OPTION_VALUES = TRAIT_OPTION_VALUES + list(df[TRAIT].unique())




def get_snipe_criteria(df_snipe):
    if COLLECTION == "jankyheist":
        price_thres_dic = {
            "Level 7": 0.25, "Level 6": 1, "Level 5": 0.7, "Level 4": 1,
            "Level 3": 0.4, "Level 2": 1, "Level 1": 1,
        }
        price_thresholds = df_snipe["Trait_Jankyness_Level"].apply(lambda x: price_thres_dic[x])
         #only fixed price listings that are below the set thresholds per Jankyness Level
        snipe_criteria = ((df_snipe["payment_token_symbol"]=="ETH") \
                            & (df_snipe["starting_price"]<= price_thresholds) )
        return snipe_criteria
    if COLLECTION == "clonex":
        price_threshold = 2.5 #flat threshold
        snipe_criteria = ((df_snipe["payment_token_symbol"]=="ETH") \
                    & (df_snipe["starting_price"]<= price_threshold))
        return snipe_criteria

def path_to_image_html(path):
    '''
     This function essentially convert the image url to 
     '<img src="'+ path + '"/>' format. And one can put any
     formatting adjustments to control the height, aspect ratio, size etc.
     within as in the below example. 
    '''

    return '<img src="'+ path + '" style=max-height:50px;"/>'




st.header(f'Dashboard - {COLLECTION}')
st.subheader(f"Floor by Trait: {TRAIT}")
st.dataframe(df_fixed_price_listings[["current_price", TRAIT]].groupby([TRAIT]).min()[["current_price"]].T)

# get timestamp
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
st.subheader(f"Recent Listings: {TRAIT}")
st.write("Refreshed at ", current_time)


df_snipe = df_rec_listings.merge(df, on="token_id")
df_snipe["snipe"] = ""

snipe_criteria = get_snipe_criteria(df_snipe)
df_snipe.loc[snipe_criteria, "snipe"] = "!!!!!!!!! SNIPE !!!!!!!!!"
df_snipe["listed_seconds_ago"] = ((df_snipe["created_date"] - pd.to_datetime(datetime.utcnow())).astype("timedelta64[s]")).astype(int)

table_columns = ["image_thumbnail_url", "token_id", "snipe", "created_date","listed_seconds_ago", TRAIT, "starting_price", "payment_token_symbol", "last_sale_price", "rarity_rank", "rarity_score" ,"permalink"]

st.write(
    df_snipe[table_columns].to_html(escape=False, formatters=dict(image_thumbnail_url=path_to_image_html)), 
    unsafe_allow_html=True
)

st.subheader(f"Last Sale Price [ETH]")

trait_value = st.selectbox(
    'Choose the trait value filter on',
    TRAIT_OPTION_VALUES
)
if trait_value:
    df_temp = df[df[TRAIT]==trait_value]
else:
    df_temp = df

fig = px.scatter(
    df_temp, x=df_temp.event_timestamp, y="last_sale_price", 
    template="plotly_dark", hover_data=["token_id"],
#    range_x=("2021-12-29T19:00:37.957817"	, pd.to_datetime(datetime.utcnow()))
)
st.write(fig)
'''
st.subheader(f"Listing Price Distribution [ETH]")
outlier_threshold_low, outlier_threshold_high = st.slider(
    "Remove outlier in the range", 
    min_value=int(df_fixed_price_listings["current_price"].min()),
    max_value=int(df_fixed_price_listings["current_price"].max()),
    value=(0, 100)
    )

df_r = df[
    (df["current_price"]<outlier_threshold_high) &
    (df["current_price"]>outlier_threshold_low) &
    (df[TRAIT] == trait_value)
]
hover_data_tip = ["token_id", "rarity_rank", "current_price", "last_sale_price"]
fig = px.box(df_r, x="current_price", points="all", 
             title="Boxplot Price Listing (Outliers ETH Removed)", template="plotly_dark", 
             hover_data=hover_data_tip
)
st.write(fig)
'''
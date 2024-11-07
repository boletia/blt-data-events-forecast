import os
import requests
import json
import joblib
import datetime
import math
import time
import pickle
import pandas as pd
import numpy as np
import streamlit as st
import snowflake.connector
from geopy.distance import geodesic

""" Snowflake connection """

USER = str(os.getenv("SNOWFLAKE_USER", ''))
PASSWORD = str(os.getenv("SNOWFLAKE_PASSWORD", ''))
ACCOUNT = str(os.getenv("SNOWFLAKE_ACCOUNT", ''))
WAREHOUSE = str(os.getenv("SNOWFLAKE_WAREHOUSE", ''))
DATABASE = str(os.getenv("SNOWFLAKE_DATABASE", ''))
SCHEMA = str(os.getenv("SNOWFLAKE_SCHEMA", ''))
DEPLOY_LOCAL = str(os.getenv("DEPLOY_LOCAL", '')).lower() == 'true'

ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)
cur = ctx.cursor()



""" Function to preprocess data"""
def preprocess_data(event_data, preprocessor_num=1):

    if preprocessor_num == 1:
        # Load and use the original model preprocessor object
        with open('preprocessor.pkl', 'rb') as f:
            preprocessor = pickle.load(f)
    
    else:
        # Load and use the synthetic data model preprocessor object
        with open('preprocessor2.pkl', 'rb') as f:
            preprocessor = pickle.load(f)

    
    preprocessed_data = preprocessor.transform(event_data)

    return preprocessed_data


""" Get Google Maps venue data """
@st.cache_data(persist="disk")
def get_venues_data():
    # Execute a query to extract the data
    sql = f"""select
                name,
                state,
                city,
                ne_lat,
                ne_lon,
                sw_lat,
                sw_lon,
                rating as venue_rating,
                user_ratings_total as venue_total_ratings,
                capacity as venue_capacity,
                CONCAT(name, ' (', city, ', ', state, ')') AS venue
            from core.places
            """
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except Exception as e:
        print("Error ejecutando la consulta:", str(e))
        return pd.DataFrame()
    

""" Get demographics data from INEGI """
def get_inegi_data(state):
    # Execute a query to extract the data
    sql = f"""select
                sum(total_population) as state_population,
                avg(pct_30) as pct_30,
                avg(pct_50) as pct_50,
                avg(pct_70) as pct_70
            from demographics.income_by_city
            where state = '{state}'
            """
    #print(sql)
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()


""" Function to authenticate Chartmetric API """
def cm_auth():
    url = 'https://api.chartmetric.com'
    response = requests.post(f'{url}/api/token', json={"refreshtoken": str(os.getenv("CM_APIKEY", ''))})
    if response.status_code != 200:
        print(f'ERROR: received a {response.status_code} instead of 200 from /api/token')
    access_token = response.json()['token']
    headers={'Authorization': f'Bearer {access_token}'}
    return headers


""" Generic call to Chartmetric API """
def cm_api_call(url, headers):
    response = requests.get(url, headers=headers)

    while response.status_code == 502:
        time.sleep(2)
        print(" 502 Error received, trying again")
        response = requests.get(url, headers=headers)
        time.sleep(2)

    while response.status_code == 429:
        print(" Rate limit exceeded, waiting a second...")
        time.sleep(1)
        response = requests.get(url, headers=headers)

    if float(response.headers['X-Response-Time'][0:-2]) < 1000:
            sleep_time = 1300 - float(response.headers['X-Response-Time'][0:-2])
            time.sleep(sleep_time/1000)

    return response


""" Search for artists based on a name"""
@st.cache_data
def cm_search_artist(artist_name, headers):
    try:
        url = f"https://api.chartmetric.com/api/search?q={artist_name}&type=artists&limit=5"
        response = cm_api_call(url, headers)

        return response.json()['obj']['artists']
    
    except:
        return None


""" Get Spotify artist listeners in MX"""
@st.cache_data
def cm_sp_listeners(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/where-people-listen?since=2024-01-01&latest=true"
        response = cm_api_call(url, headers)

        return response.json()['obj']['countries']['Mexico'][0]['listeners']
    
    except:
        return None
    

""" Get Spotify artist followers """
@st.cache_data
def cm_sp_metrics(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/spotify?latest=true"
        response = cm_api_call(url, headers)
        popularity = response.json()['obj']['popularity'][0]['value']
        followers_to_listeners = response.json()['obj']['followers_to_listeners_ratio'][0]['value']

        return followers_to_listeners, popularity
    
    except:
        return None, None
    

""" Get Instagram artist followers """
@st.cache_data
def cm_ig_metrics(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/instagram?latest=true"
        response = cm_api_call(url, headers)
        followers = response.json()['obj']['followers'][0]['value']
    
        return followers

    except:
        return None
    

""" Get youtube metrics """
@st.cache_data
def cm_yt_metrics(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/youtube_channel?latest=true"
        response = cm_api_call(url, headers)
        subscribers = response.json()['obj']['subscribers'][0]['value']
        views = response.json()['obj']['views'][0]['value']

        return subscribers, views
    
    except:
        return None, None


""" Get tiktok metrics """
@st.cache_data
def cm_tt_metrics(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/tiktok?latest=true"
        response = cm_api_call(url, headers)
        followers = response.json()['obj']['followers'][0]['value']
        likes = response.json()['obj']['likes'][0]['value']

        return followers, likes
    
    except:
        return None, None


""" Get artist chartmetric data """
@st.cache_data
def get_cm_data(cm_id):
    headers = cm_auth()
    cm_data = pd.DataFrame()

    # Spotify data
    cm_data["SP_MONTHLY_LISTENERS_MX"] = [cm_sp_listeners(cm_id, headers)]
    sp_followers_to_listeners, sp_popularity = cm_sp_metrics(cm_id, headers)
    cm_data["SP_FOLLOWERS_TO_LISTENERS_RATIO"] = [sp_followers_to_listeners]
    cm_data["SP_POPULARITY"] = sp_popularity

    # Instagram data
    ig_followers = cm_ig_metrics(cm_id, headers)
    cm_data["IG_FOLLOWERS"] = [ig_followers]

    # Youtube data
    yt_subs, views = cm_yt_metrics(cm_id, headers)
    cm_data["YT_SUBSCRIBERS"] = [yt_subs]
    cm_data["YT_VIEWS"] = [views]

    # Tiktok data
    tt_followers, tt_likes = cm_tt_metrics(cm_id, headers)
    cm_data["TT_FOLLOWERS"] = [tt_followers]
    cm_data["TT_LIKES"] = [tt_likes]
    
    return cm_data


""" Assemble the dataframe for the original model preprocessor """
def get_dataframe(venue_data, inegi_data, artist_data, min_ticket_price, 
                                 avg_ticket_price, max_ticket_price, total_tickets_on_sale, total_face_value):
    venue_data = venue_data.reset_index(drop=True)
    inegi_data = inegi_data.reset_index(drop=True)
    artist_data = artist_data.reset_index(drop=True)
    df = pd.DataFrame()
    df['VENUE_RATING'] = venue_data['VENUE_RATING']
    df['VENUE_TOTAL_RATINGS'] = venue_data['VENUE_TOTAL_RATINGS']
    df['STATE_POPULATION'] = inegi_data['STATE_POPULATION'].astype(int)
    df['TICKET_PCT_30'] = avg_ticket_price / inegi_data['PCT_30'].astype(float) * 100
    df['TICKET_PCT_50'] = avg_ticket_price / inegi_data['PCT_50'].astype(float) * 100
    df['TICKET_PCT_70'] = avg_ticket_price / inegi_data['PCT_70'].astype(float) * 100
    df['SP_MONTHLY_LISTENERS_MX'] = artist_data['SP_MONTHLY_LISTENERS_MX']
    df['SP_MONTHLY_LISTENERS_STATE'] = None if df.iloc[0]['SP_MONTHLY_LISTENERS_MX'] is None else round(df['SP_MONTHLY_LISTENERS_MX'].astype(int) * df['STATE_POPULATION'].astype(int) / 127500000)
    df['SP_FOLLOWERS_TO_LISTENERS_RATIO'] = artist_data['SP_FOLLOWERS_TO_LISTENERS_RATIO'].astype(float)
    df['SP_POPULARITY'] = artist_data['SP_POPULARITY']
    df['IG_FOLLOWERS'] = artist_data['IG_FOLLOWERS']
    df['YT_SUBSCRIBERS'] = artist_data['YT_SUBSCRIBERS']
    df['YT_VIEWS'] = artist_data['YT_VIEWS']
    df['TT_FOLLOWERS'] = artist_data['TT_FOLLOWERS']
    df['TT_LIKES'] = artist_data['TT_LIKES']
    df['TOTAL_TICKETS_ON_SALE'] = total_tickets_on_sale
    df['TOTAL_FACE_VALUE'] = total_face_value
    df['MIN_TICKET_PRICE'] = min_ticket_price
    df['AVERAGE_TICKET_PRICE'] = avg_ticket_price
    df['MAX_TICKET_PRICE'] = max_ticket_price
    return df
    

""" Assemble the dataframe for the synthetics data model preprocessor """
def get_dataframe2(venue_data, inegi_data, artist_data, min_ticket_price, 
                                 avg_ticket_price, max_ticket_price, total_tickets_on_sale, total_face_value):
    venue_data = venue_data.reset_index(drop=True)
    inegi_data = inegi_data.reset_index(drop=True)
    artist_data = artist_data.reset_index(drop=True)
    df = pd.DataFrame()
    df['VENUE_TOTAL_RATINGS'] = venue_data['VENUE_TOTAL_RATINGS']
    df['TICKET_PCT_30'] = avg_ticket_price / inegi_data['PCT_30'].astype(float) * 100
    df['TICKET_PCT_50'] = avg_ticket_price / inegi_data['PCT_50'].astype(float) * 100
    df['TICKET_PCT_70'] = avg_ticket_price / inegi_data['PCT_70'].astype(float) * 100
    df['SP_FOLLOWERS_TO_LISTENERS_RATIO'] = artist_data['SP_FOLLOWERS_TO_LISTENERS_RATIO'].astype(float)
    df['SP_POPULARITY'] = artist_data['SP_POPULARITY']
    df['IG_FOLLOWERS'] = artist_data['IG_FOLLOWERS']
    df['YT_SUBSCRIBERS'] = artist_data['YT_SUBSCRIBERS']
    df['AVERAGE_TICKET_PRICE'] = avg_ticket_price
    df['MIN_TICKET_PRICE'] = min_ticket_price
    df['MAX_TICKET_PRICE'] = max_ticket_price
    df['TICKETS_TARGET'] = total_tickets_on_sale
    df['SALES_TARGET'] = total_face_value
    return df
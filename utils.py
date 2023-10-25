import os
import json
import joblib
import datetime
import pandas as pd
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


""" Margin of Error values """
margin_of_error_regular_model = 285
margin_of_error_simple_model = 212


""" Fuction to transform numeric data using the scaler """

def normalize(numeric_data, scaler_name):
    # Read the scaler from the pickel file
    scaler = joblib.load(scaler_name)

    # Apply the trasnformation
    normalized_data = scaler.transform(numeric_data)

    return normalized_data


""" Get the day of the yaer in a date (1 - 365) """

def day_of_year(date):
    start_of_year = datetime.date(date.year, 1, 1)
    delta = date - start_of_year
    return delta.days + 1


""" List with the possible music genres of the events """

music_genres = ["Acustico", "Alternativa", "Blues y Jazz", 
                #"Clasica",
                "DJ / Dance / Electronica",
                "Experimental", "Folklorica", "Hip-Hop / Rap / Batallas de Rap", "Indie", "K-Pop",
                "Metal", 
                #"Opera",
                "Otro", "Pop",
                #"Psicodelico",
                "Punk / Hardcore", "Reggae",
                "Religioso / Espiritual", "Rock", "Tropical"]


""" List with the possible commercial values for the organizer of the event """

commercial_values = ["medium", "micro", "nano", "small", "super_top", "top"]


""" List with the days of the week """

days = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]


""" List with the months of the year """

months = ["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]


""" List with the possible type/gender of the artist """

artist_type_genders =["Group", "female", "male"]


""" Get the area of the venue from the north east and south west points coordinates """

def get_area(ne_lat, ne_lon, sw_lat, sw_lon):
    ne_coords = (ne_lat, ne_lon)
    sw_coords = (sw_lat, sw_lon)
        
    width = geodesic(ne_coords, (ne_coords[0], sw_coords[1])).meters
    height = geodesic(ne_coords, (sw_coords[0], ne_coords[1])).meters

    return width * height


""" Get Google Maps venue data """

@st.cache
def get_venue_data(place_id):
    # Execute a query to extract the data
    sql = f"""select
                name,
                state,
                city,
                ne_lat,
                ne_lon,
                sw_lat,
                sw_lon,
                rating,
                user_ratings_total
            from core.places
            where place_id = '{place_id}'
            """
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()
    

""" Get demographics data from INEGI """

@st.cache
def get_inegi_data(city, state):
    # Execute a query to extract the data
    sql = f"""select
                pct_10,
                pct_30,
                pct_50,
                pct_70,
                pct_90,
                pct_95,
                pct_lower_class,
                pct_lower_middle_class,
                pct_upper_middle_class,
                pct_upper_class,
                total_population,
                male_population/total_population as male_population_pct,
                female_population/total_population as female_population_pct
            from demographics.income_by_city
            where city = '{city}'
            and state = '{state}'
            """
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()
    

""" Get artist chartmetric data """

@st.cache
def get_cm_data(chartmetric_id):
    # Execute a query to extract the data
    sql = f"""
            select distinct
                artist_name,
                artist_id,
                COALESCE(FIRST_VALUE(cm_artist_rank) IGNORE NULLS OVER (PARTITION BY artist_name ORDER BY created_at DESC), 999999) AS cm_rank,
                COALESCE(FIRST_VALUE(cm_artist_country_rank[0]:artist_rank::string) IGNORE NULLS OVER (PARTITION BY artist_name ORDER BY created_at DESC), '999999') AS country_rank,
                COALESCE(FIRST_VALUE(cm_artist_genre_rank[0]:artist_rank) IGNORE NULLS OVER (PARTITION BY artist_name ORDER BY created_at DESC), 999999) AS genre_rank,
                COALESCE(FIRST_VALUE(cm_artist_subgenre_rank[0]:artist_rank) IGNORE NULLS OVER (PARTITION BY artist_name ORDER BY created_at DESC), 999999) AS subgenre_rank
            from raw.chartmetric.events_historical_ranks
            where chartmetric_id = {chartmetric_id}
            """
    
    #print(sql)
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()
    

""" Get artist fan metrics data """

@st.cache
def get_fm_data(chartmetric_id):
    # Execute a query to extract the data
    sql = f"""
            select
                distinct id,
                COALESCE(FIRST_VALUE(spotify_followers) IGNORE NULLS OVER (ORDER BY timestamp DESC), 0) AS spotify_followers,
                COALESCE(FIRST_VALUE(spotify_popularity) IGNORE NULLS OVER (ORDER BY timestamp DESC), 0) AS spotify_popularity,
                COALESCE(FIRST_VALUE(spotify_listeners) IGNORE NULLS OVER (ORDER BY timestamp DESC), 0) AS spotify_listeners,
                COALESCE(FIRST_VALUE(spotify_followers_to_listeners_ratio) IGNORE NULLS OVER (ORDER BY timestamp DESC), 0) AS spotify_followers_to_listeners_ratio,
                COALESCE(FIRST_VALUE(facebook_likes) IGNORE NULLS OVER (ORDER BY timestamp DESC), 0) AS facebook_likes,
                COALESCE(FIRST_VALUE(facebook_talks) IGNORE NULLS OVER (ORDER BY timestamp DESC), 0) AS facebook_talks,
                COALESCE(FIRST_VALUE(youtube_channel_views) IGNORE NULLS OVER (ORDER BY timestamp DESC), 0) AS youtube_channel_views
            from artists.fan_metrics
            where id = {chartmetric_id}
            """
    
    #print(sql)
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()
    

""" Get artist instagram data """

@st.cache
def get_ig_data(chartmetric_id):
    # Execute a query to extract the data
    sql = f"""
            select
                distinct chartmetric_id,
                COALESCE(FIRST_VALUE(top_countries) IGNORE NULLS OVER (ORDER BY timestp DESC), '') AS ig_top_countries,
                COALESCE(FIRST_VALUE(TO_DECIMAL(IFF(GET(parse_json(AUDIENCE_GENDERS), 0):code = 'female', GET(parse_json(AUDIENCE_GENDERS), 0):weight, GET(parse_json(AUDIENCE_GENDERS), 1):weight), 10, 2)) IGNORE NULLS OVER (ORDER BY timestp DESC), 0) AS ig_female_audience,
                COALESCE(FIRST_VALUE(TO_DECIMAL(IFF(GET(parse_json(AUDIENCE_GENDERS), 0):code = 'male', GET(parse_json(AUDIENCE_GENDERS), 0):weight, GET(parse_json(AUDIENCE_GENDERS), 1):weight), 10, 2)) IGNORE NULLS OVER (ORDER BY timestp DESC), 0) AS ig_male_audience,
                COALESCE(FIRST_VALUE(followers) IGNORE NULLS OVER (ORDER BY timestp DESC), 0) AS ig_followers,
                COALESCE(FIRST_VALUE(avg_likes_per_post) IGNORE NULLS OVER (ORDER BY timestp DESC), 0) AS ig_avg_likes,
                COALESCE(FIRST_VALUE(avg_commments_per_post) IGNORE NULLS OVER (ORDER BY timestp DESC), 0) AS ig_avg_comments
            from raw.chartmetric.ig_audience_data
            where chartmetric_id = {chartmetric_id}
            """
    
    #print(sql)
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()
    

""" Get the MX metrics of IG stats """

def get_ig_mx(json_str):
    
    # Initialize values
    followers_mx = 0
    percent_mx = 0

    try: 
        # Get the json data in a list of dictionaries
        data = json.loads(json_str)

        # Iterate the dictinaries
        for item in data:
            if item.get("code") == "MX":
                followers_mx = item.get("followers")
                percent_mx = item.get("percent")
                break  # Finish the loop
    except:
        print("Error extrayendo valores de json:", json_str)

    return followers_mx, percent_mx


""" Get artist gender type """

@st.cache
def get_artist_gender_type(artist_id):
    # Execute a query to extract the data
    sql = f"""
            select
                case
                    when artist_type = 'Group' then 'Group'
                    when artist_type = 'Person' then gender
                    else null
                end as artist_type_gender
            from core.artists
            where artist_id = '{artist_id}'
            """
    
    print(sql)
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()
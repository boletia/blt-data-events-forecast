import os
import requests
import json
import joblib
import datetime
import math
import time
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


""" Function to query event data using event_id """
@st.cache_data
def query_event_data(event_id):
    
    # Execute a query to extract the data
    sql = f"""
            with events_base as (
                select
                    e.*,
                    ea.artist_id,
                    upper(coalesce(a.name, ea.artist_name)) as artist_name,
                    ag.genre_name as artist_genre,
                    sp.monthly_listeners,
                    ss.ig_engagement_rate,
                    ss.yt_engagement_rate,
                    ss.tt_engagement_rate,
                    fmg.sp_followers,
                    fmg.sp_listeners,
                    fmg.sp_followers_to_listeners_ratio,
                    fmg.sp_popularity,
                    fmg.ig_followers,
                    fmg.yt_subscribers,
                    fmg.yt_views,
                    fmg.yt_videos,
                    fmg.tt_followers,
                    fmg.tt_likes,
                    fmm.ig_followers as ig_followers_mx,
                    fmm.yt_subscribers as yt_subscribers_mx,
                    fmm.tt_followers as tt_followers_mx,
                    row_number() over (partition by e.event_id order by fmg.sp_popularity desc, fmg.sp_listeners desc, fmg.sp_followers desc, try_cast(ea.artist_id as integer)) as artist_number -- REVISAR CRITERIO PARA ELEGIR ARTISTA DE CADA EVENTO
                from prod.core.event_artists as ea
                join prod.core.events as e on e.event_id = ea.event_id
                join prod.artists.artists_test as a on a.artist_id = ea.artist_id
                left join prod.artists.artist_genres as ag on ag.artist_id = a.artist_id and ag.artist_genre_number = 0
                left join prod.artists.spotify_monthly_listeners as sp on sp.artist_id = a.artist_id and sp.location_name = 'Mexico' and date(dateadd(month, -2, e.created_at)) = sp.date -- de momento x pais;
                left join prod.artists.social_audience_stats_temp as ss on ss.artist_id = a.artist_id and ss.date = date(dateadd(month, -2, e.created_at))
                left join prod.artists.fan_metrics as fmg on fmg.artist_id = a.artist_id and fmg.date = date(dateadd(month, -2, e.created_at)) and fmg.location_type = 'Global'
                left join prod.artists.fan_metrics as fmm on fmm.artist_id = a.artist_id and fmm.date = date(dateadd(month, -2, e.created_at)) and fmm.location_type = 'Country' and fmm.location_code = 'MX'
                where true
                and e.subcategory = 'Conciertos' -- solo conciertos (no festivales)
                and ea.relation_type = 'Principal' -- solo eventos donde los artistas fueron principales (no tributos, covers o artistas invitados)
                and year(e.created_at) >= 2020 -- eventos de 2020 en adelante
                order by e.event_id, artist_number
            ),

            event_payments as (
                select
                    e.event_id,
                    e.artist_genre as event_genre,
                    v.state as event_state,
                    b.booking_id,
                    --p.cc_state,
                    case
                        when cc_state in ('CDMX','Ciudad de México ','Cdmx','Ciudad de Mexico','CDMX ','Ciudad de mexico','CIUDAD DE MEXICO','Cdmx ','cdmx','Distrito Federal','ciudad de mexico') then 'Ciudad de México'
                        when cc_state in ('México','Estado de México ','México ','Estado de Mexico','Mexico ','MEXICO', 'Mexico','ESTADO DE MEXICO','mexico','Estado de mexico') then 'Estado de México'
                        when cc_state in ('Jalisco ','JALISCO','jalisco') then 'Jalisco'
                        when cc_state in ('Nuevo León ','Nuevo Leon','Nuevo leon','NUEVO LEON') then 'Nuevo León'
                        when cc_state in ('Veracruz de Ignacio de la Llave','Veracruz ') then 'Veracruz'
                        when cc_state in ('Coahuila de Zaragoza') then 'Coahuila'
                        when cc_state in ('Michoacán de Ocampo','Michoacán ') then 'Michoacán'
                        when cc_state in ('Guanajuato ') then 'Guanajuato'
                        when cc_state in ('Querétaro ','Queretaro','Queretaro ') then 'Querétaro'
                        when cc_state in ('Baja California ') then 'Baja California'
                        when cc_state in ('Chihuahua ') then 'Chihuahua'
                        when cc_state in ('Puebla ') then 'Puebla'
                        else cc_state
                    end as cc_state_fix,
                    p.cc_brand,
                    row_number() over (partition by p.booking_id order by p.attempt_id desc) as payment_number
                from events_base as e
                join prod.core.places as v on v.place_id = e.gmaps_place_id
                join prod.core.bookings as b on b.event_id = e.event_id
                join prod.payments.bookings_payments as p on p.booking_id = b.booking_id
                where e.artist_number = 1
                and b.status = 'complete'   
            ),

            genre_foreign_sales as (
                select
                    event_genre,
                    sum(case when upper(event_state) = upper(cc_state_fix) then 1 end) as local_sales,
                    sum(case when upper(event_state) != upper(cc_state_fix) then 1 end) as foreign_sales,
                    foreign_sales / (local_sales + foreign_sales) as genre_foreign_sales_pct
                from event_payments as ep
                where payment_number = 1
                and event_genre is not null
                and cc_state_fix is not null
                group by event_genre    
            ),

            state_foreign_sales as (
                select
                    event_state,
                    sum(case when upper(event_state) = upper(cc_state_fix) then 1 end) as local_sales,
                    sum(case when upper(event_state) != upper(cc_state_fix) then 1 end) as foreign_sales,
                    foreign_sales / (local_sales + foreign_sales) as state_foreign_sales_pct
                from event_payments as ep
                where payment_number = 1
                and event_state is not null
                and cc_state_fix is not null
                group by event_state  
            ),

            genre_cards as (
                select
                    event_genre,
                    sum(case when cc_brand =  'Debit' then 1 end) / count(*) as genre_debit_card_sales_pct,
                    sum(case when cc_brand =  'Traditional' then 1 end) / count(*) as genre_traditional_card_sales_pct,
                    sum(case when cc_brand =  'Gold' then 1 end) / count(*) as genre_gold_card_sales_pct,
                    sum(case when cc_brand =  'Platinum' then 1 end) / count(*) as genre_platinum_card_sales_pct,
                    sum(case when cc_brand =  'american express' then 1 end) / count(*) as genre_amex_card_sales_pct,
                from event_payments as ep
                where payment_number = 1
                and event_genre is not null
                and cc_brand is not null
                group by event_genre    
            ),

            state_cards as (
                select
                    event_state,
                    sum(case when cc_brand =  'Debit' then 1 end) / count(*) as state_debit_card_sales_pct,
                    sum(case when cc_brand =  'Traditional' then 1 end) / count(*) as state_traditional_card_sales_pct,
                    sum(case when cc_brand =  'Gold' then 1 end) / count(*) as state_gold_card_sales_pct,
                    sum(case when cc_brand =  'Platinum' then 1 end) / count(*) as state_platinum_card_sales_pct,
                    sum(case when cc_brand =  'american express' then 1 end) / count(*) as state_amex_card_sales_pct,
                from event_payments as ep
                where payment_number = 1
                and event_state is not null
                and cc_brand is not null
                group by event_state  
            ),

            genre_convertion_rates as (
                select
                    eb.artist_genre,
                    sum(case when page_target = 'finish' then users end) / sum(case when page_target = 'landing_page' then users end) as genre_avg_convertion_rate
                from events_base as eb 
                join prod.marketing.sales_funnels as sf on eb.event_id = sf.event_id
                where eb.artist_number = 1
                group by eb.artist_genre
            ),

            events_guests as (
                select
                    eb.event_id,
                    upper(eb.artist_name) as artist_name, 
                    count(*) as guests
                from events_base as eb
                left join prod.core.event_artists as ea
                on eb.event_id = ea.event_id and upper(eb.artist_name) != upper(ea.artist_name)
                where ea.relation_type in ('Principal', 'Invitado')
                and eb.artist_number = 1
                group by 1,2
            ),

            songkick_events_by_genre as (
                select distinct
                    g.genre_name,
                    upper(e.artist_name) as artist_name,
                    e.event_date,
                    e.state
                from prod.artists.artist_events as e
                join prod.artists.artist_genres as g on upper(e.artist_name) = upper(g.artist_name)
                where e.state is not null
            ),

            agg as (
                select
                    -- Event 
                    e.event_id,
                    e.name as event_name,
                    date(dateadd(month, -2, e.created_at)) as event_planning_date,
                    date(e.created_at) as event_created_at,
                    date(e.started_at) as event_started_at, -- desglosar por mes, dia de la semana, dia del año
                    dayofweek(e.started_at) as event_dayofweek_start,
                    month(e.started_at) as event_start_month,
                    e.adv_category_event_subcategory,
                
                    -- Venue
                    e.gmaps_place_id as venue_id,
                    v.name as venue_name,
                    v.rating as venue_rating,
                    v.user_ratings_total as venue_total_ratings,
                    v.capacity as venue_capacity,
                    v.ne_lat,
                    v.ne_lon,
                    v.sw_lat,
                    v.sw_lon,
                
                    -- Demographics
                    v.city, -- considerar city de locations?
                    v.state,
                    sfs.state_foreign_sales_pct,
                    sc.state_debit_card_sales_pct,
                    sc.state_traditional_card_sales_pct,
                    sc.state_gold_card_sales_pct,
                    sc.state_platinum_card_sales_pct,
                    sc.state_amex_card_sales_pct,
                    i.total_population / i.households as city_avg_people_per_house,
                    i.total_population as city_population,
                    i.female_population / i.total_population as female_population_pct,
                    i.male_population / i.total_population as male_population_pct,
                    i.pop_0_11 / i.total_population as pop_0_11_pct,
                    i.pop_12_17 / i.total_population as pop_12_17_pct,
                    i.pop_18_24 / i.total_population as pop_18_24_pct,
                    i.pop_25_34 / i.total_population as pop_25_34_pct,
                    i.pop_35_44 / i.total_population as pop_35_44_pct,
                    i.pop_45_64 / i.total_population as pop_45_64_pct,
                    i.pop_65_and_more / i.total_population as pop_65_and_more_pct,
                    i.pct_10,
                    i.pct_30,
                    i.pct_50,
                    i.pct_70,
                    i.pct_90,
                    i.pct_95,
                
                    -- Artists
                    e.artist_id,
                    e.artist_name,
                    e.artist_genre,
                    e.monthly_listeners as sp_monthly_listeners_mx, -- en mexico
                    round(e.monthly_listeners * i.total_population / 126700000) as sp_monthly_listeners_city, -- cálculo por ciudad
                    e.sp_followers, -- datos globales a partir de aqui
                    e.sp_listeners,
                    round(e.sp_followers_to_listeners_ratio, 2) as sp_followers_to_listeners_ratio,
                    e.sp_popularity,
                    e.ig_followers,
                    e.ig_followers_mx,
                    round(e.ig_followers_mx * i.total_population / 126700000) as ig_followers_city,
                    e.yt_subscribers,
                    e.yt_subscribers_mx,
                    round(e.yt_subscribers_mx * i.total_population / 126700000) as yt_subscribers_city,
                    e.yt_views,
                    e.yt_videos,
                    e.tt_followers,
                    e.tt_followers_mx,
                    round(e.tt_followers_mx * i.total_population / 126700000) as tt_followers_city,
                    e.tt_likes,

                    -- Genre stats
                    gcr.genre_avg_convertion_rate,
                    gfs.genre_foreign_sales_pct,
                    gc.genre_debit_card_sales_pct,
                    gc.genre_traditional_card_sales_pct,
                    gc.genre_gold_card_sales_pct,
                    gc.genre_platinum_card_sales_pct,
                    gc.genre_amex_card_sales_pct,
                
                    -- Tickets
                    tt.ticket_type_id,
                    tt.name as ticket_type_name,
                    tt.quantity as ticket_type_quantity,
                    tt.price as ticket_type_price,
                    case when tt.name ilike '%general%' then true else false end as general_ticket,
                    case when tt.name ilike '%vip%' or tt.name ilike '%v.i.p.%' then true else false end as vip_ticket,
                    case when tt.name ilike '%meet%' or tt.name ilike '%greet%' then true else false end as meet_and_greet_ticket
                    
                from events_base as e
                left join prod.core.places as v on v.place_id = e.gmaps_place_id
                left join prod.demographics.income_by_city as i on i.city = v.city and i.state = v.state
                left join prod.core.ticket_types as tt on tt.event_id = e.event_id
                left join genre_foreign_sales as gfs on gfs.event_genre = e.artist_genre
                left join state_foreign_sales as sfs on sfs.event_state = v.state
                left join genre_cards as gc on gc.event_genre = e.artist_genre
                left join state_cards as sc on sc.event_state = v.state
                left join genre_convertion_rates as gcr on gcr.artist_genre = e.artist_genre
                where e.artist_number = 1
                and tt.price > 0 -- se excluyen cortesias
            ),

            event_similar_events as (
                select
                    agg.event_id,
                    count(distinct se.artist_name) as similar_events
                from agg
                join songkick_events_by_genre as se
                on se.genre_name = agg.artist_genre
                and agg.state = se.state
                and se.event_date between dateadd(day, -30, agg.event_started_at) and dateadd(day, 30, agg.event_started_at)
                and agg.artist_name != se.artist_name -- ya estan en mayusculas
                group by 1
            ),

            sales as (
                select
                    t.ticket_type_id,
                    count(distinct t.ticket_id) as tickets_sold,
                    sum(t.face_value) as tickets_face_value
                from events_base as e
                left join prod.core.bookings as b on b.event_id = e.event_id
                left join prod.core.tickets as t on b.booking_id = t.booking_id
                where e.artist_number = 1
                and b.status = 'complete'
                group by 1
            ),

            final as (
                select 
                    agg.*,
                    coalesce(se.similar_events, 0) as similar_events,
                    coalesce(eg.guests, 0) as guests,
                    coalesce(s.tickets_sold, 0) as tickets_sold,
                    div0(coalesce(s.tickets_sold, 0), agg.ticket_type_quantity) as ticket_type_sold_out,
                    agg.ticket_type_price * coalesce(s.tickets_sold, 0) as tickets_calculated_face_value,
                    --coalesce(s.tickets_face_value, 0) as tickets_face_value,
                    --coalesce(s.tickets_total, 0) as tickets_total
                from agg
                left join event_similar_events as se on se.event_id = agg.event_id
                left join events_guests as eg on eg.event_id = agg.event_id and agg.artist_name = eg.artist_name
                left join sales as s on s.ticket_type_id = agg.ticket_type_id
                order by agg.event_created_at, agg.event_id, agg.artist_name
            ) 

            select * from final where event_id = {event_id};
            """
    
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except Exception as e:
        print("Error ejecutando la consulta:", str(e))
        return pd.DataFrame()


""" Function to preprocess data"""
def preprocess_data(event_data, aditional_columns=False):

    if aditional_columns == True:
        columns_to_drop = [ 'EVENT_ID',
                            'EVENT_NAME',
                            'EVENT_PLANNING_DATE',
                            'EVENT_CREATED_AT',
                            'EVENT_STARTED_AT',
                            'ADV_CATEGORY_EVENT_SUBCATEGORY',
                            'VENUE_ID',
                            'VENUE_NAME',
                            'ARTIST_ID',
                            'ARTIST_NAME',
                            'NE_LAT',
                            'NE_LON',
                            'SW_LAT',
                            'SW_LON',
                            'TICKET_TYPE_ID',
                            'TICKET_TYPE_NAME',
                            'CITY',
                            'STATE',
                            
                            'ARTIST_GENRE',
                            'TICKETS_SOLD',
                            'TICKET_TYPE_SOLD_OUT',
                            'TICKETS_CALCULATED_FACE_VALUE'
                        ]
        
        event_data = event_data.drop(columns_to_drop, axis=1)

    

    # Load and use the preprocessor object
    preprocessor = joblib.load("preprocessor.pkl")
    preprocessed_data = preprocessor.transform(event_data)

    return preprocessed_data


""" Get Google Maps venue data """
@st.cache_data
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
@st.cache_data
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
    print(sql)
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
    

# Get youtube metrics
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


# Get tiktok metrics
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
    cm_ig_metrics(cm_id, headers)
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


""" Assemble the dataframe for the preprocessor """
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
    

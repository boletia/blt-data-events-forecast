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
    
    # Generate the venue area data (m^2) from the gmaps coordinates
    areas = []
    for _, row in event_data.iterrows():
        ne_coords = (row['NE_LAT'], row['NE_LON'])
        sw_coords = (row['SW_LAT'], row['SW_LON'])
        
        # Check if the values are not NaN
        if not (math.isnan(ne_coords[0]) or math.isnan(ne_coords[1]) or
                math.isnan(sw_coords[0]) or math.isnan(sw_coords[1])):
            
            width = geodesic(ne_coords, (ne_coords[0], sw_coords[1])).meters
            height = geodesic(ne_coords, (sw_coords[0], ne_coords[1])).meters
            
            area_m2 = width * height
            areas.append(area_m2)
        else:
            areas.append(None)

    event_data['VENUE_AREA'] = areas

    # Create columns of the percentage of income that represents a ticket
    event_data['TICKET_PCT_10'] = event_data['TICKET_TYPE_PRICE'].astype(float) / event_data['PCT_10'].astype(float) * 100
    event_data['TICKET_PCT_30'] = event_data['TICKET_TYPE_PRICE'].astype(float) / event_data['PCT_30'].astype(float) * 100
    event_data['TICKET_PCT_50'] = event_data['TICKET_TYPE_PRICE'].astype(float) / event_data['PCT_50'].astype(float) * 100
    event_data['TICKET_PCT_70'] = event_data['TICKET_TYPE_PRICE'].astype(float) / event_data['PCT_70'].astype(float) * 100
    event_data['TICKET_PCT_90'] = event_data['TICKET_TYPE_PRICE'].astype(float) / event_data['PCT_90'].astype(float) * 100
    event_data['TICKET_PCT_95'] = event_data['TICKET_TYPE_PRICE'].astype(float) / event_data['PCT_95'].astype(float) * 100

    # Create column of city population percentage of country
    event_data['CITY_POPULATION_PCT'] = event_data['CITY_POPULATION'] / 127500000

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
    else:
        columns_to_drop = ['NE_LAT',
                           'NE_LON',
                           'SW_LAT',
                           'SW_LON'
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
                concat(name, ' (', city, ', ', state, ')') as venue
            from core.places
            """
    try:
        cur.execute(sql)
        # Converting data into a dataframe
        df = cur.fetch_pandas_all()
        return df
    
    except:
        return pd.DataFrame()
    

""" Get demographics data from INEGI """
@st.cache_data
def get_inegi_data(city, state):
    # Execute a query to extract the data
    sql = f"""select
                total_population / households as city_avg_people_per_house,
                total_population as city_population,
                female_population / total_population as female_population_pct,
                male_population / total_population as male_population_pct,
                pop_0_11 / total_population as pop_0_11_pct,
                pop_12_17 / total_population as pop_12_17_pct,
                pop_18_24 / total_population as pop_18_24_pct,
                pop_25_34 / total_population as pop_25_34_pct,
                pop_35_44 / total_population as pop_35_44_pct,
                pop_45_64 / total_population as pop_45_64_pct,
                pop_65_and_more / total_population as pop_65_and_more_pct,
                pct_10,
                pct_30,
                pct_50,
                pct_70,
                pct_90,
                pct_95
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
        followers = response.json()['obj']['followers'][0]['value']
        popularity = response.json()['obj']['popularity'][0]['value']
        listeners = response.json()['obj']['listeners'][0]['value']
        followers_to_listeners = response.json()['obj']['followers_to_listeners_ratio'][0]['value']

        return followers, listeners, followers_to_listeners, popularity
    
    except:
        return None, None, None, None
    

""" Get Instagram artist followers """
@st.cache_data
def cm_ig_metrics(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/instagram?latest=true"
        response = cm_api_call(url, headers)
        followers = response.json()['obj']['followers'][0]['value']

        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/instagram?latest=true&code2=MX"
        response = cm_api_call(url, headers)
        followers_mx = response.json()['obj']['followers'][0]['value']
    
        return followers, followers_mx

    except:
        return None, None
    

# Get youtube metrics
@st.cache_data
def cm_yt_metrics(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/youtube_channel?latest=true"
        response = cm_api_call(url, headers)
        subscribers = response.json()['obj']['subscribers'][0]['value']
        views = response.json()['obj']['views'][0]['value']
        videos = response.json()['obj']['videos'][0]['value']
        
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/youtube_channel?latest=true&code2=MX"
        response = cm_api_call(url, headers)
        subscribers_mx = response.json()['obj']['subscribers'][0]['value']

        return subscribers, subscribers_mx, views, videos
    
    except:
        return None, None, None, None


# Get tiktok metrics
@st.cache_data
def cm_tt_metrics(cm_id, headers):
    try:
        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/tiktok?latest=true"
        response = cm_api_call(url, headers)
        followers = response.json()['obj']['followers'][0]['value']
        likes = response.json()['obj']['likes'][0]['value']

        url = f"https://api.chartmetric.com/api/artist/{cm_id}/stat/tiktok?latest=true&code2=MX"
        response = cm_api_call(url, headers)
        followers_mx = response.json()['obj']['followers'][0]['value']

        return followers, followers_mx, likes
    
    except:
        return None, None, None


""" Get artist chartmetric data """
@st.cache_data
def get_cm_data(cm_id):
    headers = cm_auth()
    cm_data = pd.DataFrame()

    # Spotify data
    cm_data["SP_MONTHLY_LISTENERS_MX"] = [cm_sp_listeners(cm_id, headers)]
    sp_followers, sp_listeners, sp_followers_to_listeners, sp_popularity = cm_sp_metrics(cm_id, headers)
    cm_data["SP_FOLLOWERS"] = [sp_followers]
    cm_data["SP_LISTENERS"] = [sp_listeners]
    cm_data["SP_FOLLOWERS_TO_LISTENERS_RATIO"] = [sp_followers_to_listeners]
    cm_data["SP_POPULARITY"] = sp_popularity

    # Instagram data
    cm_ig_metrics(cm_id, headers)
    ig_followers, ig_followers_mx = cm_ig_metrics(cm_id, headers)
    cm_data["IG_FOLLOWERS"] = [ig_followers]
    cm_data["IG_FOLLOWERS_MX"] = [ig_followers_mx]

    # Youtube data
    yt_subs, yt_subs_mx, views, videos = cm_yt_metrics(cm_id, headers)
    cm_data["YT_SUBSCRIBERS"] = [yt_subs]
    cm_data["YT_SUBSCRIBERS_MX"] = [yt_subs_mx]
    cm_data["YT_VIEWS"] = [views]
    cm_data["YT_VIDEOS"] = [videos]

    # Tiktok data
    tt_followers, tt_followers_mx, tt_likes = cm_tt_metrics(cm_id, headers)
    cm_data["TT_FOLLOWERS"] = [tt_followers]
    cm_data["TT_FOLLOWERS_MX"] = [tt_followers_mx]
    cm_data["TT_LIKES"] = [tt_likes]
    
    return cm_data


""" Assemble the dataframe for the preprocessor """
def get_dataframe(dayofweek_start, start_month, venue_data, inegi_data, artist_data, guests, ticket_type_price, ticket_type_quantity, ticket_type):
    df = pd.DataFrame()
    df['EVENT_DAYOFWEEK_START'] = [dayofweek_start]
    df['EVENT_START_MONTH'] = [start_month]
    df['VENUE_RATING'] = venue_data['VENUE_RATING']
    df['VENUE_TOTAL_RATINGS'] = venue_data['VENUE_TOTAL_RATINGS']
    df['VENUE_CAPACITY'] = venue_data['VENUE_CAPACITY']
    df['NE_LAT'] = venue_data['NE_LAT']
    df['NE_LON'] = venue_data['NE_LON']
    df['SW_LAT'] = venue_data['SW_LAT']
    df['SW_LON'] = venue_data['SW_LON']
    df['STATE_FOREIGN_SALES_PCT'] = None
    df['STATE_DEBIT_CARD_SALES_PCT'] = None
    df['STATE_TRADITIONAL_CARD_SALES_PCT'] = None
    df['STATE_GOLD_CARD_SALES_PCT'] = None
    df['STATE_PLATINUM_CARD_SALES_PCT'] = None
    df['STATE_AMEX_CARD_SALES_PCT'] = None
    df['CITY_AVG_PEOPLE_PER_HOUSE'] = inegi_data['CITY_AVG_PEOPLE_PER_HOUSE']
    df['CITY_POPULATION'] = inegi_data['CITY_POPULATION']
    df['FEMALE_POPULATION_PCT'] = inegi_data['FEMALE_POPULATION_PCT']
    df['MALE_POPULATION_PCT'] = inegi_data['MALE_POPULATION_PCT']
    df['POP_0_11_PCT'] = inegi_data['POP_0_11_PCT']
    df['POP_12_17_PCT'] = inegi_data['POP_12_17_PCT']
    df['POP_18_24_PCT'] = inegi_data['POP_18_24_PCT']
    df['POP_25_34_PCT'] = inegi_data['POP_25_34_PCT']
    df['POP_35_44_PCT'] = inegi_data['POP_35_44_PCT']
    df['POP_45_64_PCT'] = inegi_data['POP_45_64_PCT']
    df['POP_65_AND_MORE_PCT'] = inegi_data['POP_65_AND_MORE_PCT']
    df['PCT_10'] = inegi_data['PCT_10']
    df['PCT_30'] = inegi_data['PCT_30']
    df['PCT_50'] = inegi_data['PCT_50']
    df['PCT_70'] = inegi_data['PCT_70']
    df['PCT_90'] = inegi_data['PCT_90']
    df['PCT_95'] = inegi_data['PCT_95']
    df['SP_MONTHLY_LISTENERS_MX'] = artist_data['SP_MONTHLY_LISTENERS_MX']
    df['SP_MONTHLY_LISTENERS_CITY'] = None if df.iloc[0]['SP_MONTHLY_LISTENERS_MX'] is None else round(df['SP_MONTHLY_LISTENERS_MX'] * df['CITY_POPULATION'] / 127500000)
    df['SP_FOLLOWERS'] = artist_data['SP_FOLLOWERS']
    df['SP_LISTENERS'] = artist_data['SP_LISTENERS']
    df['SP_FOLLOWERS_TO_LISTENERS_RATIO'] = artist_data['SP_FOLLOWERS_TO_LISTENERS_RATIO']
    df['SP_POPULARITY'] = artist_data['SP_POPULARITY']
    df['IG_FOLLOWERS'] = artist_data['IG_FOLLOWERS']
    df['IG_FOLLOWERS_MX'] = artist_data['IG_FOLLOWERS_MX']
    df['IG_FOLLOWERS_CITY'] = None if df.iloc[0]['IG_FOLLOWERS_MX'] is None else round(df['IG_FOLLOWERS_MX'] * df['CITY_POPULATION'] / 127500000)
    df['YT_SUBSCRIBERS'] = artist_data['YT_SUBSCRIBERS']
    df['YT_SUBSCRIBERS_MX'] = artist_data['YT_SUBSCRIBERS_MX']
    df['YT_SUBSCRIBERS_CITY'] = None if df.iloc[0]['YT_SUBSCRIBERS_MX'] is None else round(df['YT_SUBSCRIBERS_MX'] * df['CITY_POPULATION'] / 127500000)
    df['YT_VIEWS'] = artist_data['YT_VIEWS']
    df['YT_VIDEOS'] = artist_data['YT_VIDEOS']
    df['TT_FOLLOWERS'] = artist_data['TT_FOLLOWERS']
    df['TT_FOLLOWERS_MX'] = artist_data['TT_FOLLOWERS_MX']
    df['TT_FOLLOWERS_CITY'] = None if df.iloc[0]['TT_FOLLOWERS_MX'] is None else round(df['TT_FOLLOWERS_MX'] * df['CITY_POPULATION'] / 127500000)
    df['TT_LIKES'] = artist_data['TT_LIKES']
    df['GENRE_AVG_CONVERTION_RATE'] = None
    df['GENRE_FOREIGN_SALES_PCT'] = None
    df['GENRE_DEBIT_CARD_SALES_PCT'] = None
    df['GENRE_TRADITIONAL_CARD_SALES_PCT'] = None
    df['GENRE_GOLD_CARD_SALES_PCT'] = None
    df['GENRE_PLATINUM_CARD_SALES_PCT'] = None
    df['GENRE_AMEX_CARD_SALES_PCT'] = None
    df['TICKET_TYPE_QUANTITY'] = ticket_type_quantity
    df['TICKET_TYPE_PRICE'] = ticket_type_price
    df['GENERAL_TICKET'] = True if ticket_type == 'General' else False
    df['VIP_TICKET'] = True if ticket_type == 'VIP' else False
    df['MEET_AND_GREET_TICKET'] = True if ticket_type == 'Meet and Greet' else False
    df['SIMILAR_EVENTS'] = None
    df['GUESTS'] = guests

    return df
    

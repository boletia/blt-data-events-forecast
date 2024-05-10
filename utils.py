import os
import json
import joblib
import datetime
import math
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
                    sum(t.face_value) as tickets_face_value,
                    sum(t.ticket_total) as tickets_total
                from events_base as e
                left join prod.core.tickets as t on t.event_id = e.event_id
                where e.artist_number = 1
                and t.booking_status = 'complete'
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
    
    except:
        return pd.DataFrame()


""" Function to preprocess data"""

def preprocess_data(event_data):
    
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
    event_data['TICKET_PCT_10'] = event_data['TICKET_TYPE_PRICE'] / event_data['PCT_10'] * 100
    event_data['TICKET_PCT_30'] = event_data['TICKET_TYPE_PRICE'] / event_data['PCT_30'] * 100
    event_data['TICKET_PCT_50'] = event_data['TICKET_TYPE_PRICE'] / event_data['PCT_50'] * 100
    event_data['TICKET_PCT_70'] = event_data['TICKET_TYPE_PRICE'] / event_data['PCT_70'] * 100
    event_data['TICKET_PCT_90'] = event_data['TICKET_TYPE_PRICE'] / event_data['PCT_90'] * 100
    event_data['TICKET_PCT_95'] = event_data['TICKET_TYPE_PRICE'] / event_data['PCT_95'] * 100

    # Create column of city population percentage of country
    event_data['CITY_POPULATION_PCT'] = event_data['CITY_POPULATION'] / 127500000

    # Drop not required columns
    columns_to_drop = ['EVENT_ID',
                    'EVENT_NAME',
                    'EVENT_PLANNING_DATE',
                    'EVENT_CREATED_AT',
                    'EVENT_STARTED_AT',
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
                    'ADV_CATEGORY_EVENT_SUBCATEGORY',
                    'ARTIST_GENRE'
                    ]

    event_data = event_data.drop(columns_to_drop, axis=1)

    # Load and use the preprocessor object
    preprocessor = joblib.load("preprocessor.pkl")
    preprocessed_data = preprocessor.transform(event_data)

    return preprocessed_data


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
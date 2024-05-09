import streamlit as st
import json
import requests
import pandas as pd
import boto3
import utils
import mlflow
import joblib
from datetime import datetime
from mlflow.deployments import get_deploy_client
from millify import prettify


def main():

    # Check the deploy variable to load local models or use sagemaker 
    if utils.DEPLOY_LOCAL:
        # load local models
        model = joblib.load('model.pkl')
        st.write("Se está usando un modelo en modo local *")
    else:
        # use sagemaker client 
        client = get_deploy_client(f"sagemaker:/us-east-1")
        st.write("Se está usando un modelo via Sagemaker API *")


    st.title("Forecast de Eventos")


    # Radio button to select how to enter the data
    #option = st.radio("Selecciona una opción:", ("Buscar datos con ID de evento", "Ingresar datos manualmente"))
    option = "Buscar datos con ID de evento"

    if option == "Buscar datos con ID de evento":
        event_id = st.text_input("ID de evento:")
        event_data = utils.query_event_data(event_id)

        if event_data.empty:
            st.warning(f"No hay datos del evento: {event_id}")

        else:
            st.write("\n")
            st.subheader("Datos del evento y boletos")
            st.write(event_data)

            # Drop target variables and preprocess the data
            event_data = event_data.drop(['TICKETS_SOLD','TICKET_TYPE_SOLD_OUT','TICKETS_CALCULATED_FACE_VALUE'], axis=1)
            preprocessed_data = utils.preprocess_data(event_data)
            st.write("\n")

        
    elif option == "Ingresar datos manualmente":
        st.write("Ingresa los datos del evento")

        # if st.checkbox("¿Es un evento numerado?"):
        #     is_numbered = True
        # else: 
        #     is_numbered = False

        # if st.checkbox("¿Tiene habilitado MSI?"):
        #     has_msi = True
        # else: 
        #     has_msi = False
        
        # Calendar to select the event sale date
        sale_date = st.date_input("Fecha en que se pondrá a la venta")
        
        # Calendar to select the event date
        selected_date = st.date_input("Fecha en que se llevará a cabo")

        # Get some variables from the dates
        if selected_date:
            #dayofweek_start = utils.days[selected_date.weekday()] 
            #start_month = utils.months[selected_date.month-1]
            start_week = selected_date.strftime("%U")  # week of the year
            start_day = utils.day_of_year(selected_date)
        else:
            #dayofweek_start = None
            #start_month = None
            start_week = None
            start_day = None
        
        if sale_date and selected_date:
            # Converte streamlit dates into datetime objects
            sale_date = datetime(sale_date.year, sale_date.month, sale_date.day)
            selected_date = datetime(selected_date.year, selected_date.month, selected_date.day)
        
            # Calculate difference between dates
            lead_time_days = (selected_date - sale_date).days

        #subgenre = st.selectbox("Subgénero musical", utils.music_genres)
        commercial_value = st.selectbox("Tamaño del organizador", utils.commercial_values)

        # Venue data
        st.subheader("Datos del venue")
        gmaps_place_id = st.text_input("Google Maps ID del venue")

        # Initialize variables
        venue_name = ""
        state = ""
        city = ""
        ne_lat = 0
        ne_lon = 0
        sw_lat = 0
        sw_lon = 0
        venue_area = 0
        venue_rating = 0
        venue_ratings_total = 0

        # Query the venue data
        venue_data = utils.get_venue_data(gmaps_place_id)
        #st.write(venue_data)

        # Check if the query returned data
        if not venue_data.empty:
            venue_name = venue_data["NAME"][0]
            state = venue_data["STATE"][0]
            city = venue_data["CITY"][0]
            ne_lat = venue_data["NE_LAT"][0]
            ne_lon = venue_data["NE_LON"][0]
            sw_lat = venue_data["SW_LAT"][0]
            sw_lon = venue_data["SW_LON"][0]
            venue_area = utils.get_area(ne_lat, ne_lon, sw_lat, sw_lon)
            venue_rating = venue_data["RATING"][0]
            venue_ratings_total = venue_data["USER_RATINGS_TOTAL"][0]
            
            st.write("Venue:", venue_name)
            st.write("Estado:", state)
            st.write("Ciudad:", city)
            st.write("Rating:", venue_rating)
            st.write("Total de ratings de los usuarios:", venue_ratings_total)
        
        else:
            st.warning("No hay datos disponibles del venue.")

        
        # Demographics data from INEGI
        inegi_data = utils.get_inegi_data(city, state)
        #st.write(inegi_data)
        
        # Check if the query returned data
        if not inegi_data.empty:
            pct_10 = inegi_data["PCT_10"][0]
            pct_30 = inegi_data["PCT_30"][0]
            pct_50 = inegi_data["PCT_50"][0]
            pct_70 = inegi_data["PCT_70"][0]
            pct_90 = inegi_data["PCT_90"][0]
            pct_95 = inegi_data["PCT_95"][0]
            pct_lower_class = inegi_data["PCT_LOWER_CLASS"][0]
            pct_lower_middle_class = inegi_data["PCT_LOWER_MIDDLE_CLASS"][0]
            pct_upper_middle_class = inegi_data["PCT_UPPER_MIDDLE_CLASS"][0]
            pct_upper_class = inegi_data["PCT_UPPER_CLASS"][0]
            total_population = inegi_data["TOTAL_POPULATION"][0]
            female_population_pct = inegi_data["FEMALE_POPULATION_PCT"][0]
            male_population_pct = inegi_data["MALE_POPULATION_PCT"][0]

            st.subheader("Datos de INEGI")
            st.write("Ingresos del 10 percentil:  $ ", prettify(round(pct_10)), "MXN")
            st.write("Ingresos del 30 percentil:  $ ", prettify(round(pct_30)), "MXN")
            st.write("Ingresos del 50 percentil:  $ ", prettify(round(pct_50)), "MXN")
            st.write("Ingresos del 70 percentil:  $ ", prettify(round(pct_70)), "MXN")
            st.write("Ingresos del 90 percentil:  $ ", prettify(round(pct_90)), "MXN")
            st.write("Ingresos del 95 percentil:  $ ", prettify(round(pct_95)), "MXN")
            st.write("Población de clase baja:", round(pct_lower_class*100, 2), "%")
            st.write("Población de clase media baja:", round(pct_lower_middle_class*100, 2), "%")
            st.write("Población de clase media alta:", round(pct_upper_middle_class*100, 2), "%")
            st.write("Población de clase alta:", round(pct_upper_class*100, 2), "%")
            st.write("Población total:", prettify(total_population), "personas")
            st.write("Porcentaje de población femenina:", round(female_population_pct*100, 2), "%")
            st.write("Porcentaje de población masculina:", round(male_population_pct*100, 2), "%")
        
        else:
            pct_10 = 0
            pct_30 = 0
            pct_50 = 0
            pct_70 = 0
            pct_90 = 0
            pct_95 = 0
            pct_lower_class = 0
            pct_lower_middle_class = 0
            pct_upper_middle_class = 0
            pct_upper_class = 0
            total_population = 0
            female_population_pct = 0
            male_population_pct = 0
            st.warning("No hay datos de INEGI disponibles.")


        # Ticket data
        st.subheader("Datos del tipo de boleto")
        ticket_type_price = st.number_input("Precio", min_value=100, step=500)
        ticket_type_quantity = st.number_input("Cantidad de boletos a la venta", min_value=10, step=100)
        

        if not inegi_data.empty:
            ticket_pct_10 = ticket_type_price / pct_10 * 100
            ticket_pct_30 = ticket_type_price / pct_30 * 100
            ticket_pct_50 = ticket_type_price / pct_50 * 100
            ticket_pct_70 = ticket_type_price / pct_70 * 100
            ticket_pct_90 = ticket_type_price / pct_90 * 100
            ticket_pct_95 = ticket_type_price / pct_95 * 100

            st.subheader("Que % del salario representa un boleto?")
            st.write("Para los ingresos del percentil 10:", round(ticket_pct_10, 2), "%")
            st.write("Para los ingresos del percentil 30:", round(ticket_pct_30, 2), "%")
            st.write("Para los ingresos del percentil 50:", round(ticket_pct_50, 2), "%")
            st.write("Para los ingresos del percentil 70:", round(ticket_pct_70, 2), "%")
            st.write("Para los ingresos del percentil 90:", round(ticket_pct_90, 2), "%")
            st.write("Para los ingresos del percentil 95:", round(ticket_pct_95, 2), "%")
        
        else:
            ticket_pct_10 = 0
            ticket_pct_30 = 0
            ticket_pct_50 = 0
            ticket_pct_70 = 0
            ticket_pct_90 = 0
            ticket_pct_95 = 0

        
        # Chartmetric artist data
        st.subheader("Datos del artista")
        chartmetric_id = int(st.number_input("Chartmetric ID del artista", min_value=1, step=1))

        artist_data = utils.get_cm_data(chartmetric_id)
        #st.write(artist_data)

        artist_type_gender = None
        cm_rank = 999999
        country_rank = 999999
        genre_rank = 999999
        subgenre_rank = 999999
        ig_female_audience = 0
        ig_male_audience = 0
        ig_followers = 0
        ig_avg_likes = 0
        ig_avg_comments = 0
        ig_followers_mx = 0
        ig_percent_mx = 0
        spotify_followers = 0
        spotify_popularity = 0
        spotify_listeners = 0
        spotify_followers_to_listeners_ratio = 0
        facebook_likes = 0
        facebook_talks = 0
        youtube_channel_views = 0

        # Check if the query returned data
        if not artist_data.empty:
            artist_name = artist_data['ARTIST_NAME'][0]
            artist_id = artist_data["ARTIST_ID"][0]
            cm_rank = artist_data['CM_RANK'][0]
            country_rank = artist_data['COUNTRY_RANK'][0]
            genre_rank = artist_data['GENRE_RANK'][0]
            subgenre_rank = artist_data['SUBGENRE_RANK'][0]
            type_gender_df = utils.get_artist_gender_type(artist_id)
            artist_type_gender = type_gender_df['ARTIST_TYPE_GENDER'][0]

            st.write("Nombre:", artist_name)
            st.write("Tipo/género:", artist_type_gender)
            st.write("CM rank:", prettify(cm_rank))
            st.write("Country rank:", country_rank)
            st.write("Genre rank:", genre_rank)
            st.write("Subgenre rank:", subgenre_rank)

        else:
            st.warning("No hay datos del artista en chartmetric.")


        # Fan metrics data
        fm_data = utils.get_fm_data(chartmetric_id)
        #st.write(fm_data)

        # Check if the query returned data
        if not fm_data.empty:
            spotify_followers = fm_data["SPOTIFY_FOLLOWERS"][0]
            spotify_popularity = fm_data["SPOTIFY_POPULARITY"][0]
            spotify_listeners = fm_data["SPOTIFY_LISTENERS"][0]
            spotify_followers_to_listeners_ratio = fm_data["SPOTIFY_FOLLOWERS_TO_LISTENERS_RATIO"][0]
            facebook_likes = fm_data["FACEBOOK_LIKES"][0]
            facebook_talks = fm_data["FACEBOOK_TALKS"][0]
            youtube_channel_views = fm_data["YOUTUBE_CHANNEL_VIEWS"][0]

            st.write("Spotify followers: ", prettify(spotify_followers))
            st.write("Spotify popularity: ", prettify(spotify_popularity))
            st.write("Spotify listeners: ", prettify(spotify_listeners))
            st.write("Spotify followers/listeners: ", spotify_followers_to_listeners_ratio)
            st.write("Facebook likes: ", prettify(facebook_likes))
            st.write("Facebook talks: ", prettify(facebook_talks))
            st.write("Youtube views: ", prettify(youtube_channel_views))
        
        else:
            st.warning("No hay datos del artista en fan metrics.")

        
        # IG data
        ig_data = utils.get_ig_data(chartmetric_id)
        #st.write(ig_data)

        # Check if the query returned data
        if not ig_data.empty:
            ig_female_audience = ig_data["IG_FEMALE_AUDIENCE"][0]
            ig_male_audience = ig_data["IG_MALE_AUDIENCE"][0]
            ig_followers = ig_data["IG_FOLLOWERS"][0]
            ig_avg_likes = ig_data["IG_AVG_LIKES"][0]
            ig_avg_comments = ig_data["IG_AVG_COMMENTS"][0]
            ig_top_countries = ig_data["IG_TOP_COUNTRIES"][0]
            ig_followers_mx, ig_percent_mx = utils.get_ig_mx(ig_top_countries)

            st.write("IG female audience:", ig_female_audience, "%")
            st.write("IG male audience:", ig_male_audience, "%")
            st.write("IG followers:", prettify(ig_followers))
            st.write("IG avg likes:", prettify(ig_avg_likes))
            st.write("IG avg commentss:", prettify(ig_avg_comments))
            st.write("IG followers MX:", prettify(ig_followers_mx))
            st.write("IG percent MX:", ig_percent_mx, "%")
        else:
            st.warning("No hay datos de IG del artista.")

        
        # Create dummy variables for categorical data
        #subgenre_dummies = {f"SUBCATEGORY_{genre}": True if genre == subgenre else False for genre in utils.music_genres}
        commercial_value_dummies = {f"COMMERCIAL_VALUE_{cvalue}": True if cvalue == commercial_value else False for cvalue in utils.commercial_values}
        #dayofweek_start_dummies = {f"DAYOFWEEK_START_{day}": True if day == dayofweek_start else False for day in utils.days}
        #start_month_dummies = {f"SUBCATEGORY_{month}": True if month == start_month else False for month in utils.months}
        #artist_type_gender_dummies = {f"ARTIST_TYPE_GENDER_{type_gender}": True if type_gender == artist_type_gender else False for type_gender in utils.artist_type_genders}

        numeric_data = pd.DataFrame({
            "LEAD_TIME_DAYS": [lead_time_days],
            "START_WEEK": [start_week],
            "START_DAY": [start_day],
            "VENUE_RATING": [venue_rating],
            "VENUE_RATINGS_TOTAL": [venue_ratings_total],
            "CM_RANK": [cm_rank],
            "COUNTRY_RANK": [country_rank],
            "GENRE_RANK": [genre_rank],
            "SUBGENRE_RANK": [subgenre_rank],
            "IG_FEMALE_AUDIENCE" : [ig_female_audience],
            "IG_MALE_AUDIENCE": [ig_male_audience],
            "IG_FOLLOWERS": [ig_followers],
            "IG_AVG_LIKES": [ig_avg_likes],
            "IG_AVG_COMMENTS": [ig_avg_comments],
            "SPOTIFY_FOLLOWERS": [spotify_followers],
            "SPOTIFY_POPULARITY": [spotify_popularity],
            "SPOTIFY_LISTENERS": [spotify_listeners],
            "SPOTIFY_FOLLOWERS_TO_LISTENERS_RATIO": [spotify_followers_to_listeners_ratio],
            "FACEBOOK_LIKES": [facebook_likes],
            "FACEBOOK_TALKS": [facebook_talks],
            "YOUTUBE_CHANNEL_VIEWS": [youtube_channel_views],
            "PCT_10": [pct_10],
            "PCT_30": [pct_30],
            "PCT_50": [pct_50],
            "PCT_70": [pct_70],
            "PCT_90": [pct_90],
            "PCT_95": [pct_95],
            "PCT_LOWER_CLASS": [pct_lower_class],
            "PCT_LOWER_MIDDLE_CLASS": [pct_lower_middle_class],
            "PCT_UPPER_MIDDLE_CLASS": [pct_upper_middle_class],
            "PCT_UPPER_CLASS": [pct_upper_class],
            "TOTAL_POPULATION": [total_population],
            "MALE_POPULATION_PCT": [male_population_pct],
            "FEMALE_POPULATION_PCT": [female_population_pct],
            "TICKET_TYPE_PRICE": [ticket_type_price],
            "TICKET_TYPE_QUANTITY": [ticket_type_quantity],
            "IG_FOLLOWERS_MX": [ig_followers_mx],
            "IG_PERCENT_MX": [ig_percent_mx],
            "VENUE_AREA": [venue_area],
            "TICKET_PCT_10": [ticket_pct_10],
            "TICKET_PCT_30": [ticket_pct_30],
            "TICKET_PCT_50": [ticket_pct_50],
            "TICKET_PCT_70": [ticket_pct_70],
            "TICKET_PCT_90": [ticket_pct_90],
            "TICKET_PCT_95": [ticket_pct_95]
        })

        # Normalize numeric data
        st.subheader("Dataframe")
        st.write(numeric_data)
        normalized_numeric_data = utils.normalize(numeric_data, 'scaler.pkl')
        normalized_numeric_values = normalized_numeric_data.tolist()[0]

        # Set the payload for the API call
        payload_data = {
            "dataframe_split": {
                "columns": [
                    #"IS_NUMBERED",
                    #"HAS_MSI",
                    "LEAD_TIME_DAYS",
                    "START_WEEK",
                    "START_DAY",
                    "VENUE_RATING",
                    "VENUE_RATINGS_TOTAL",
                    "CM_RANK",
                    "COUNTRY_RANK",
                    "GENRE_RANK",
                    "SUBGENRE_RANK",
                    "IG_FEMALE_AUDIENCE",
                    "IG_MALE_AUDIENCE",
                    "IG_FOLLOWERS",
                    "IG_AVG_LIKES",
                    "IG_AVG_COMMENTS",
                    "SPOTIFY_FOLLOWERS",
                    "SPOTIFY_POPULARITY",
                    "SPOTIFY_LISTENERS",
                    "SPOTIFY_FOLLOWERS_TO_LISTENERS_RATIO",
                    "FACEBOOK_LIKES",
                    "FACEBOOK_TALKS",
                    "YOUTUBE_CHANNEL_VIEWS",
                    "PCT_10",
                    "PCT_30",
                    "PCT_50",
                    "PCT_70",
                    "PCT_90",
                    "PCT_95",
                    "PCT_LOWER_CLASS",
                    "PCT_LOWER_MIDDLE_CLASS",
                    "PCT_UPPER_MIDDLE_CLASS",
                    "PCT_UPPER_CLASS",
                    "TOTAL_POPULATION",
                    "MALE_POPULATION_PCT",
                    "FEMALE_POPULATION_PCT",
                    "TICKET_TYPE_PRICE",
                    "TICKET_TYPE_QUANTITY",
                    "IG_FOLLOWERS_MX",
                    "IG_PERCENT_MX",
                    "VENUE_AREA",
                    "TICKET_PCT_10",
                    "TICKET_PCT_30",
                    "TICKET_PCT_50",
                    "TICKET_PCT_70",
                    "TICKET_PCT_90",
                    "TICKET_PCT_95",
                    #*list(subgenre_dummies.keys()),
                    #*list(dayofweek_start_dummies.keys()),
                    #*list(start_month_dummies.keys()),
                    *list(commercial_value_dummies.keys())
                    #*list(artist_type_gender_dummies.keys())         
                ],
                "data": [
                        #list([is_numbered]) + 
                        #list([has_msi]) +
                        normalized_numeric_values + 
                        #list(subgenre_dummies.values()) +
                        #list(dayofweek_start_dummies.values()) +
                        #list(start_month_dummies.values()) +
                        list(commercial_value_dummies.values()) 
                        #list(artist_type_gender_dummies.values())
                        ]
            }
        }
    
    
    if st.button("Obtener Predicciones"):

        # Use local model
        if utils.DEPLOY_LOCAL:
            predictions = model.predict(preprocessed_data)

        # Use Sagemaker API model 
        else:
            # Prepare payload for API model
            payload = json.dumps(payload_data)
            # Use API sagemaker model
            prediction = client.predict('ticketsmodel', payload)
            # Make prediction from API
            predictions = prediction['predictions'][0]
            

        # Defining MAE metric
        mae = 82

        # Show predictions
        st.write("\n")
        st.subheader("Predicciones del modelo")
        predictions_df = pd.DataFrame(predictions, columns=['% Sold out'])
        predictions_df['Tickets vendidos'] = predictions_df['% Sold out'] * event_data['TICKET_TYPE_QUANTITY']
        predictions_df['Valor de tickets (MXN)'] = predictions_df['Tickets vendidos'] * event_data['TICKET_TYPE_PRICE']
        predictions_df['% Sold out'] = predictions_df['% Sold out'] * 100
        predictions_df['% Sold out'] = predictions_df['% Sold out'].round(decimals=2)
        predictions_df[['Tickets vendidos', 'Valor de tickets (MXN)']] = predictions_df[['Tickets vendidos', 'Valor de tickets (MXN)']].round()

        st.write(predictions_df)
        st.warning(f"El error promedio del modelo es de 25.63% del sold out.")


if __name__ == "__main__":
    main()
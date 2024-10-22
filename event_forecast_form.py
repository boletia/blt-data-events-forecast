import streamlit as st
import json
import requests
import pandas as pd
import boto3
import utils
import mlflow
import joblib
import pickle
from datetime import datetime
from mlflow.deployments import get_deploy_client
from millify import millify


def main():

    # Load original model
    with open('model.pkl', 'rb') as f:
        model = pickle.load(f)

    # Load synthetic data model
    with open('model2.pkl', 'rb') as f:
        model2 = pickle.load(f)

    st.title("Forecast de Eventos")
    st.write("\n")
    st.write("\n")

    # Venue and demographic data
    venues_df = utils.get_venues_data()
    selected_venue = st.selectbox("Seleccione el venue", venues_df['VENUE'])
    venue_data = venues_df[selected_venue == venues_df['VENUE']]
    inegi_data = utils.get_inegi_data(venue_data.iloc[0]['STATE'])

    # Tickets data
    c1, c2 = st.columns(2)

    with c1:
        min_ticket_price = st.number_input("Precio mínimo del boleto (MXN)", min_value=100, step=100)
    
    with c2:
        max_ticket_price = st.number_input("Precio máximo del boleto (MXN)", min_value=100, step=100)
        
    c3, c4 = st.columns(2)
    
    with c3:
        total_tickets_on_sale = st.number_input("Total de boletos a la venta", min_value=100, step=100)
    
    with c4:
        total_face_value = st.number_input("Face value total (MXN)", min_value=10000, step=100)

    avg_ticket_price = total_face_value / total_tickets_on_sale
    
    # Chartmetric artist data
    if "cm_id" not in st.session_state:
        st.session_state['cm_id'] = ''

    artist_name = st.text_input("Nombre del artista", placeholder="Luis Miguel")

    if artist_name != '':

        artists = utils.cm_search_artist(artist_name, utils.cm_auth())
        st.write('\n')
        st.write('\n')
        
        # Crear columnas
        cols = st.columns(5)

        # Mostrar información de cada artista en columnas
        for idx, artista in enumerate(artists):
            with cols[idx]:
                st.markdown(
                    f"""
                    <style>
                    .artist-image {{
                        width: 150px;
                        height: 150px;
                        object-fit: cover;
                        border-radius: 50%;
                    }}
                    </style>
                    <img src="{artista['image_url']}" class="artist-image">
                    """,
                    unsafe_allow_html=True
                )
                name = artista['name']
                if artista['verified']:
                    name += " ✅"  # Agrega una palomita al nombre si es verificado
                st.write('\n')
                st.write(f"**{name}**")
                st.write(f"**Chartmetric ID:** {artista['id']}")
                st.write(f"**Followers en Spotify:** {millify(artista['sp_followers']) if artista['sp_followers'] is not None else '-'}")
                st.write(f"**Listeners en Spotify:** {millify(artista['sp_monthly_listeners']) if artista['sp_monthly_listeners'] is not None else '-'}")
                st.write(f"**Score del artista:** {millify(artista['cm_artist_score']) if artista['cm_artist_score'] is not None else '-'}")

                if st.button("Seleccionar este artista", key=artista['id']):
                    # Se guarda en session state para mantener siempre el ultimo valor seleccionado
                    st.session_state['cm_id'] = artista['id']

    st.write('\n')
    st.write('\n')
    st.write(f"ID de artista elegido: {st.session_state['cm_id']}")
    artist_data = utils.get_cm_data(st.session_state['cm_id'])

    # Get dataframe with data for the original model
    df = utils.get_dataframe(venue_data, inegi_data, artist_data, min_ticket_price, 
                                avg_ticket_price, max_ticket_price, total_tickets_on_sale, total_face_value)
    
    # Get dataframe with data for the synthetic data model
    df2 = utils.get_dataframe2(venue_data, inegi_data, artist_data, min_ticket_price, 
                                avg_ticket_price, max_ticket_price, total_tickets_on_sale, total_face_value)
    st.write('\n')

        

    
    if st.button("Obtener Predicciones"):

        pc1, pc2 = st.columns(2)
        st.write('\n')

        # For original model
        with pc1:

            # Preprocess the data for the model
            preprocessed_data = utils.preprocess_data(df)

            # Get predictions from model
            predictions = model.predict(preprocessed_data)

            # Show predictions
            st.write("\n")
            st.subheader("MODELO ORIGINAL")
            st.subheader('Datos de entrada')
            st.write('\n')
            st.write(df)

            predictions_df = pd.DataFrame()

            st.subheader('Predicciones')
            st.write('\n')

            # Sold out %
            predictions_df['Sold out prediccion (%)'] = pd.DataFrame(predictions, columns=['Sold out prediccion (%)'])
            # Tickets sold
            predictions_df['Tickets vendidos prediccion'] = predictions_df['Sold out prediccion (%)'] * total_tickets_on_sale
            # Tickets sold value
            predictions_df['Face value total prediccion (MXN)'] = round(predictions_df['Tickets vendidos prediccion'] * avg_ticket_price)
            # Formating
            predictions_df['Sold out prediccion (%)'] = predictions_df['Sold out prediccion (%)'] * 100
            predictions_df['Sold out prediccion (%)'] = predictions_df['Sold out prediccion (%)'].round(decimals=2)
            predictions_df[['Tickets vendidos prediccion', 'Face value total prediccion (MXN)']] = predictions_df[['Tickets vendidos prediccion', 'Face value total prediccion (MXN)']].round()

            st.write(predictions_df)
            #st.warning(f"El error promedio del modelo es del 18% en el sold out.")


        # For synthetic data model
        with pc2:

            # Preprocess the data for the model
            preprocessed_data2 = utils.preprocess_data(df2, preprocessor_num=2)

            # Get predictions from model
            predictions2 = model2.predict(preprocessed_data2)

            # Show predictions
            st.write("\n")
            st.subheader("MODELO DATA SINTÉTICA")
            st.subheader('Datos de entrada')
            st.write('\n')
            st.write(df2)

            predictions_df2 = pd.DataFrame()

            st.subheader('Predicciones')
            st.write('\n')

            # Sold out %
            predictions_df2['Sold out prediccion (%)'] = pd.DataFrame(predictions2, columns=['Sold out prediccion (%)'])
            # Tickets sold
            predictions_df2['Tickets vendidos prediccion'] = predictions_df2['Sold out prediccion (%)'] * total_tickets_on_sale / 100
            # Tickets sold value
            predictions_df2['Face value total prediccion (MXN)'] = round(predictions_df2['Tickets vendidos prediccion'] * avg_ticket_price)
            # Formating
            predictions_df2['Sold out prediccion (%)'] = predictions_df2['Sold out prediccion (%)'].round(decimals=2)
            predictions_df2[['Tickets vendidos prediccion', 'Face value total prediccion (MXN)']] = predictions_df2[['Tickets vendidos prediccion', 'Face value total prediccion (MXN)']].round()

            st.write(predictions_df2)
            #st.warning(f"El error promedio del modelo es del 22% en el sold out.")



if __name__ == "__main__":
    # First streamlit line to set up the page layout
    st.set_page_config(layout="wide")

    main()
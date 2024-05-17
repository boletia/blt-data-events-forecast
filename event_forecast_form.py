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
    st.write("\n")


    # Radio button to select how to enter the data
    option = st.radio("Selecciona una opción:", ("Buscar datos con ID de evento", "Ingresar datos manualmente"))

    if option == "Buscar datos con ID de evento":
        st.write("\n")
        event_id = st.text_input("ID de evento:", placeholder='174472')
        event_data = utils.query_event_data(event_id)

        if event_data.empty:
            st.warning(f"No hay datos del evento: {event_id}")

        else:
            st.write("\n")
            st.subheader("Datos del evento y boletos")
            st.write(event_data)

            # Drop target variables and preprocess the data
            preprocessed_data = utils.preprocess_data(event_data, aditional_columns=True)
            st.write("\n")
        
        st.write("\n")

        
    elif option == "Ingresar datos manualmente":
        st.write("\n")
        
        # Calendar to select the event date
        selected_date = st.date_input("Fecha del evento")
        dayofweek_start = 0 if selected_date.weekday() == 6 else selected_date.weekday() + 1
        start_month = selected_date.month

        # Venue data
        venues_df = utils.get_venues_data()
        selected_venue = st.selectbox("Seleccione el venue", venues_df['VENUE'])
        venue_data = venues_df[selected_venue == venues_df['VENUE']]
        
        # Demographics data from INEGI
        inegi_data = utils.get_inegi_data(venue_data.iloc[0]['CITY'], venue_data.iloc[0]['STATE'])
        
        # Chartmetric artist data
        cm_id = st.text_input("Chartmetric ID del artista", placeholder='1152538')
        artist_data = utils.get_cm_data(cm_id)
        guests = st.number_input("Número de artistas invitados", min_value=0, step=1)

        # Ticket data
        ticket_type_price = st.number_input("Precio del boleto", min_value=50, step=100)
        ticket_type_quantity = st.number_input("Cantidad de boletos a la venta", min_value=50, step=100)
        ticket_type = st.radio("Selecciona el tipo de ticket:", ("General", "VIP", "Meet and Greet"))

        df = utils.get_dataframe(dayofweek_start, start_month, venue_data, inegi_data, artist_data, guests, ticket_type_price, ticket_type_quantity, ticket_type)
        st.write('\n')
        #st.write(df)
        preprocessed_data = utils.preprocess_data(df, aditional_columns=False)

        

    
    if st.button("Obtener Predicciones"):

        # Use local model
        if utils.DEPLOY_LOCAL:
            predictions = model.predict(preprocessed_data)

        # Use Sagemaker API model 
        else:
            # Prepare payload for API model
            payload = json.dumps('')
            # Use API sagemaker model
            prediction = client.predict('ticketsmodel', payload)
            # Make prediction from API
            predictions = prediction['predictions'][0]
            

        # Show predictions
        st.write("\n")
        st.subheader("Predicciones del modelo")
        predictions_df = pd.DataFrame()

        if option == "Buscar datos con ID de evento":
            # Ticket type
            predictions_df['Tipo de Boleto'] = event_data['TICKET_TYPE_NAME']
            # Sold out %
            predictions_df['Sold out prediccion (%)'] = pd.DataFrame(predictions, columns=['Sold out prediccion (%)'])
            predictions_df['Sold out real (%)'] = event_data['TICKET_TYPE_SOLD_OUT'].astype(float) * 100
            # Tickets sold
            predictions_df['Tickets vendidos prediccion'] = predictions_df['Sold out prediccion (%)'] * event_data['TICKET_TYPE_QUANTITY']
            predictions_df['Tickets vendidos real'] = event_data["TICKETS_SOLD"]
            # Tickets sold value
            predictions_df['Valor de tickets prediccion (MXN)'] = predictions_df['Tickets vendidos prediccion'] * event_data['TICKET_TYPE_PRICE']
            predictions_df['Valor de tickets real (MXN)'] = event_data["TICKETS_CALCULATED_FACE_VALUE"]
            # Formating
            predictions_df['Sold out prediccion (%)'] = predictions_df['Sold out prediccion (%)'] * 100
            predictions_df[['Sold out prediccion (%)', 'Sold out real (%)']] = predictions_df[['Sold out prediccion (%)','Sold out real (%)']].round(decimals=2)
            predictions_df[['Tickets vendidos prediccion', 'Valor de tickets prediccion (MXN)']] = predictions_df[['Tickets vendidos prediccion', 'Valor de tickets prediccion (MXN)']].round()

        else:
            # Sold out %
            predictions_df['Sold out prediccion (%)'] = pd.DataFrame(predictions, columns=['Sold out prediccion (%)'])
            # Tickets sold
            predictions_df['Tickets vendidos prediccion'] = predictions_df['Sold out prediccion (%)'] * ticket_type_quantity
            # Tickets sold value
            predictions_df['Valor de tickets prediccion (MXN)'] = predictions_df['Tickets vendidos prediccion'] * ticket_type_price
            # Formating
            predictions_df['Sold out prediccion (%)'] = predictions_df['Sold out prediccion (%)'] * 100
            predictions_df['Sold out prediccion (%)'] = predictions_df['Sold out prediccion (%)'].round(decimals=2)
            predictions_df[['Tickets vendidos prediccion', 'Valor de tickets prediccion (MXN)']] = predictions_df[['Tickets vendidos prediccion', 'Valor de tickets prediccion (MXN)']].round()


        st.write(predictions_df)
        st.warning(f"El error promedio del modelo es de 25.63% del sold out.")


if __name__ == "__main__":
    # First streamlit line to set up the page layout
    st.set_page_config(layout="wide")

    main()
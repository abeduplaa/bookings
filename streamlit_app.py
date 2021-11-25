import requests
import pandas as pd
import numpy as np

import streamlit as st
from streamlit import caching

import datetime
import math


headers = {
    'authority': "api.drivekyte.com",
    'origin': "https://fleet.drivekyte.com",
    'sec-fetch-mode': 'cors',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cookie': 'experimentation_subject_id=eyJfcmFpbHMiOnsibWVzc2FnZSI6IklqSmtPVE13TjJKbUxUTmpPR1V0TkdVd01DMWlZbVpoTFRoa1ptRTFOVFkxTWpjMk55ST0iLCJleHAiOm51bGwsInB1ciI6ImNvb2tpZS5leHBlcmltZW50YXRpb25fc3ViamVjdF9pZCJ9fQ%3D%3D--b8eafd8e117561e97e621eb178152ceb7dd9ce00; session=b9208acc-455c-48fb-a7d9-5997b7e1f023; _ga=GA1.2.1397983444.1629704284; _hjid=05d1707d-6139-420f-8f8a-8b6a569f0422; _fbp=fb.1.1629704283763.210886670; __qca=P0-63429127-1629704283912; rbuid=1497012765331692043; __adroll_fpc=20726dabc7f9002db26567a2ca3c5910-1629741904755; _scid=971d53b8-c87d-4f75-8456-c3e0fa760fa4; G_ENABLED_IDPS=google; __stripe_mid=8a542c6c-09f7-411d-9215-eeffb8fb52163976e6; __ar_v4=ZKKPBDRBP5CXVOHDYEPEIN%3A20210822%3A16%7CRO4OLXXBM5HFRMMYO2S3YV%3A20210822%3A16%7C3QY3D2AQBJA3HDQNPWMB3A%3A20210822%3A16; ab.storage.deviceId.0d58e0a0-54ca-447e-8e36-4070450fa383=%7B%22g%22%3A%226245ed38-af4e-7416-d6ab-4c3738d99e82%22%2C%22c%22%3A1632305921318%2C%22l%22%3A1632305921318%7D; mp_fb276b416f731801fe06277063a7e645_mixpanel=%7B%22distinct_id%22%3A%20%2217b71f1e4b0b96-07fefd97153c98-35607403-13c680-17b71f1e4b1dd7%22%2C%22%24device_id%22%3A%20%2217b71f1e4b0b96-07fefd97153c98-35607403-13c680-17b71f1e4b1dd7%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22%24search_engine%22%3A%20%22google%22%7D; remember_token=91209|874e6fda65e09a1efa22c39386470279b2db82c5566f72b9e13c5b72cdb8f5386caa54ea15ac6360799741cc38b7154497a52338d286ad26339128b3e4b01a92; _hjSessionUser_2348692=eyJpZCI6Ijc4NTE3YmQyLWEwZTItNWU0Yi1iNDUzLTViMWY2ZmIwMzdiNCIsImNyZWF0ZWQiOjE2MzcxNTkxMDIwNDYsImV4aXN0aW5nIjp0cnVlfQ==; ajs_user_id=%22fb8aeb42-e0d3-42be-8b7a-b125f5552bc9%22; ajs_anonymous_id=%228b80dc38-3ff2-427c-8e65-26d3aa30b836%22; ab.storage.userId.0d58e0a0-54ca-447e-8e36-4070450fa383=%7B%22g%22%3A%22fb8aeb42-e0d3-42be-8b7a-b125f5552bc9%22%2C%22c%22%3A1637240531020%2C%22l%22%3A1637240531020%7D; ab.storage.sessionId.0d58e0a0-54ca-447e-8e36-4070450fa383=%7B%22g%22%3A%2222ca447e-60dc-cb71-dd34-873e5c9595c2%22%2C%22e%22%3A1637240663139%2C%22c%22%3A1637240633140%2C%22l%22%3A1637240633140%7D; _gcl_au=1.1.1506338605.1637606960; _uetvid=0ca7ae7003e511eca86ad7229272d4df',
    
}

DATE_COLUMN = 'start_date'

# @st.cache
def load_data(start_date, end_date, selected_service_area, selected_vehicle_type):
    params = {
            'all_trips_during_window':"true",
            'deposit_collected':'false',
            'start_date':start_date,
            'end_date':end_date,
            'service_area_name':selected_service_area,
            'no_surfer_activity':'false',
            'surfer_unassigned':'false',
            'trip_status':'all',
            'user_unverified':'false',
            'user_verified':'false',
            'vehicle_unassigned':'false',
            'vehicle_classes':selected_vehicle_type,
            'limit':10000,
            'leg_type':'delivery',
        }
    
    response = requests.get("https://api.drivekyte.com/api/fleet/bookings/legs?",params=params, headers=headers)
    print(response.status_code)
    response.raise_for_status()
    response_meta = response.json()['meta']
    response_data = response.json()['data']
    
    df = pd.DataFrame(response_data)

    # convert to datetime
    df['service_area'] = df['service_area'].apply(lambda x: x['name'])
    df['start_date'] = df['trip'].apply(lambda x: x['start_date'])
    df['end_date'] = df['trip'].apply(lambda x: x['end_date'])
    
    df['start_date'] = pd.to_datetime(df['start_date'],unit='ms')
    df['end_date'] = pd.to_datetime(df['end_date'],unit='ms')
    
    # remove unnecessary data
    df = df.drop(['lot','handover_address','no_surfer_activity','supplier_handoff_time','surfer','start_time','trip'],axis=1)
    df = df.drop(df[(df['type'] == 'swap')].index)
    df = df.drop(df[(df['type'] == 'swap')].index)

    return df

    

######### STREAMLIT INPUT WIDGETS #########

### Service area selector
form = st.form("my_form")
# list_service_area = list(data['service_area'].unique())
list_service_area = ['LA', 'SF', 'BOS', 'BK', 'DC', 'NYC', 'SEA', 'MIA', 'CHI', 'LB', 'PHL']
selected_service_area = form.selectbox("Select your service area", list_service_area)
print(selected_service_area)

### Vehicle type selector
list_vehicle_type = ["Economy", "SUV", "Sedan"]
selected_vehicle_type = form.selectbox("Select vehicle type", list_vehicle_type)

### input number of vehicles 
initial_no_vehicles = form.number_input("Insert start number of vehicles from today's fleet audit. \n Keep at 0 if you don't want to calculate this",value=0)

### Start day selector
start_date_day = form.date_input("Start day", datetime.datetime.now())
start_date_time = datetime.datetime.min.time()
start_date = datetime.datetime.combine(start_date_day, start_date_time)

### End day selector
end_date_day = form.date_input("End day", datetime.datetime.now() + datetime.timedelta(days=1) )
end_date_time = datetime.datetime.min.time()
end_date = datetime.datetime.combine(end_date_day, end_date_time)

#sumbmit button
submitted = form.form_submit_button("Submit")


## once user clicks submit
if submitted:
    
    # Create a text element and let the reader know the data is loading.
    data_load_state = st.text('Loading data...')
    data = load_data(start_date, end_date, selected_service_area, selected_vehicle_type)

    # Notify the reader that the data was successfully loaded.
    data_load_state.text('Loading data...done!')
    
    # create dataframe to plot 
    df_plot = pd.DataFrame(pd.date_range(start=start_date, end=end_date,freq='H'), columns=['date'])
    df_plot['no_bookings']=0
    
    for row in df_plot.index:
        df_plot.at[row, 'no_bookings'] = ( (data['start_date'] < df_plot.at[row, 'date']) & (data['end_date'] >= df_plot.at[row, 'date']) ).sum()
    
    df_plot['no_bookings'] = df_plot['no_bookings'].astype('int')
    df_plot = df_plot.set_index('date')

    df_plot['delta'] = df_plot['no_bookings'].apply(lambda x: df_plot['no_bookings'][start_date] - x)
    
    if initial_no_vehicles != 0:
        car_col_name = "No of " + selected_vehicle_type
        df_plot[car_col_name] = df_plot['delta'].apply(lambda x: initial_no_vehicles + x)
        cols = ['no_bookings', car_col_name]
        text_subheader = "Number of ongoing bookings and available " + selected_vehicle_type + ", per hour"
    else:
        cols = ['no_bookings']
        text_subheader = "Number of ongoing " + selected_vehicle_type + " bookings, per hour"


    # create histogram
    
    st.subheader(text_subheader)
    st.line_chart(df_plot[cols])


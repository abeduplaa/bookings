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
    'cookie': st.secrets["COOKIE"]
    
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


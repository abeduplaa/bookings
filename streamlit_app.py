import requests
import pandas as pd

import streamlit as st
import time
import datetime


headers = {
    'authority': "api.drivekyte.com",
    'origin': "https://fleet.drivekyte.com",
    'sec-fetch-mode': 'cors',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'cookie': st.secrets["COOKIE"]
    
}

params = {
            'all_trips_during_window':"true",
            'deposit_collected':'false',
            'no_surfer_activity':'false',
            'surfer_unassigned':'false',
            'trip_status':'all',
            'user_unverified':'false',
            'user_verified':'false',
            'vehicle_unassigned':'false',
            'limit':10000,
        }

DATE_COLUMN = 'start_date'

def post_filter_one_ways(data, selected_service_area):
    return data.loc[data["type"]=="delivery"].loc[data["service_area"]==selected_service_area]


def utc_to_local(data, service_area):
    if service_area in ['LA', 'SF',  'SEA', 'LB',]:
        data['start_date'] = data['start_date'] - pd.Timedelta(hours=8)
        data['end_date'] = data['end_date'] - pd.Timedelta(hours=8)
    elif service_area == 'CHI':
        data['start_date'] = data['start_date'] - pd.Timedelta(hours=6)
        data['end_date'] = data['end_date'] - pd.Timedelta(hours=6)
    elif service_area in ['BOS', 'BK', 'DC', 'NYC','MIA', 'PHL', 'JC']:
        data['start_date'] = data['start_date'] - pd.Timedelta(hours=5)
        data['end_date'] = data['end_date'] - pd.Timedelta(hours=5)
    else:
        raise 
    
    return data


def load_data(start_date_utc, end_date_utc, selected_service_area, selected_vehicle_type):
    
    start_date_unix = int(start_date_utc.timestamp()*1000)
    end_date_unix = int(end_date_utc.timestamp()*1000) 

    params['start_date']= start_date_unix
    params['end_date']= end_date_unix
    params['service_area_name']=selected_service_area
    params['leg_type']='delivery'

    if selected_vehicle_type != "All":
        params['vehicle_classes']=selected_vehicle_type

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

    return df, response_meta 

    

######### STREAMLIT INPUT WIDGETS #########

### Service area selector
form = st.form("my_form")
# list_service_area = list(data['service_area'].unique())
list_service_area = ['LA', 'SF', 'BOS', 'BK', 'DC', 'NYC', 'SEA', 'MIA', 'CHI', 'LB', 'PHL', 'JC']
selected_service_area = form.selectbox("Select your service area", list_service_area)

### Vehicle type selector
list_vehicle_type = ["Economy", "SUV", "Sedan", "All"]
selected_vehicle_type = form.selectbox("Select vehicle type", list_vehicle_type)

### Start day selector
start_date_day = form.date_input("Start day", datetime.datetime.now())
start_date_time = form.time_input("Start Time", datetime.time(7, 00))
start_date = datetime.datetime.combine(start_date_day, start_date_time)


### End day selector
end_date_day = form.date_input("End day", datetime.datetime.now() + datetime.timedelta(days=1) )
end_date_time = form.time_input("End Time", datetime.time(22, 00))
end_date = datetime.datetime.combine(end_date_day, end_date_time)


### input number of vehicles 
# if vehicle_availability:
initial_no_vehicles = form.number_input("Insert start number of vehicles from today's fleet audit. \n Keep at 0 if you don't want to calculate this",value=0)

#sumbmit button
submitted = form.form_submit_button("Submit")


## once user clicks submit
if submitted:
    
    # Create a text element and let the reader know the data is loading.
    data_load_state = st.text('Loading data...')
    data, meta = load_data(start_date_utc=start_date, end_date_utc=end_date, selected_service_area=selected_service_area, selected_vehicle_type=selected_vehicle_type)
    data = utc_to_local(data, selected_service_area)
    
    # output meta 
    print(meta)

    # Notify the reader that the data was successfully loaded.
    data_load_state.text('Loading data...done!')
    

    t0 = time.time()
    # create dataframe to plot 
    df_plot = pd.DataFrame(pd.date_range(start=start_date, end=end_date,freq='15min'), columns=['date'])
    df_plot['no_bookings']=0
    
    for row in df_plot.index:
        df_plot.at[row, 'no_bookings'] = ( (data['start_date'] <= df_plot.at[row, 'date']) & (data['end_date'] > df_plot.at[row, 'date']) ).sum()
    
    df_plot['no_bookings'] = df_plot['no_bookings'].astype('int')
    df_plot = df_plot.set_index('date')

    df_plot['delta'] = df_plot['no_bookings'].apply(lambda x: df_plot['no_bookings'][start_date] - x)
    
    print(time.time() - t0)
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

    # Show bookings in time period:
    leaving_from_text = "Bookings during Time period in " + selected_service_area
    st.write(data)    


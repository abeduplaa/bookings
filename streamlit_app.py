from heapq import merge
import requests
import pandas as pd
from supply_count import *
import streamlit as st
import time
import datetime
import json

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

dict_service_area = {
    'LA': ["49666e77-91a9-47ba-ab08-ab33861f5761", "15c30fc5-9133-4166-8f6e-5b8a2d94f4d6"],
    'SF': "f18626dc-404b-4166-9c5b-dd1a450f6d1c",
    'BOS': "103d323b-4b38-476d-ab30-eaf8c92eddfe",
    'BK': "9c8dd958-1652-45d0-901b-2a7235db4d54",
    'DC': "b984f1e0-d250-40ee-b1e2-a3fd7fabc844",
    'NYC': ["42fdd319-ff38-4b52-ad60-18bee91dc55b", "d7b71559-b71d-4637-8031-140284eaec73"],
    'SEA': "b04d109a-7626-42ce-af0a-acad0134c2bc",
    'MIA': "d579cf3c-de1c-4c00-8bca-47add0578e92",
    'CHI': "ec30a116-737e-4173-ac4d-a8d8b23fc796",
    'LB': "628f1553-0f70-4b9a-80ff-1c2ead3c77a7",
    'PHL': "f3065893-beeb-41fa-8ed1-79e4c725ba4a",
    'JC': "31b7facf-063f-4816-a897-cd55aa994747",
}


selected_service_area = form.selectbox("Select your service area", list_service_area)

### Vehicle type selector
list_vehicle_type = ["Economy", "SUV", "Sedan", "All"]
selected_vehicle_type = form.selectbox("Select vehicle type", list_vehicle_type)

### Start day selector
start_date_day = form.date_input("Start day", datetime.datetime.now())
start_date_time = form.time_input("Start Time", datetime.time(7, 00))
start_date = datetime.datetime.combine(start_date_day, start_date_time)
start_date_search = datetime.datetime.combine(start_date_day, start_date_time) - pd.Timedelta(hours=8)


### End day selector
end_date_day = form.date_input("End day", datetime.datetime.now() + datetime.timedelta(days=1) )
end_date_time = form.time_input("End Time", datetime.time(22, 00))
end_date = datetime.datetime.combine(end_date_day, end_date_time)
end_date_search = datetime.datetime.combine(end_date_day, end_date_time) + pd.Timedelta(hours=8)

### input number of vehicles 
# if vehicle_availability:
initial_no_vehicles = form.number_input("Insert start number of vehicles from today's fleet audit. \n Keep at 0 if you don't want to calculate this",value=0)

#sumbmit button
submitted = form.form_submit_button("Submit")


## once user clicks submit
if submitted:
    
    data_load_state = st.text('Loading booking data...')
    data, meta = load_data(start_date_utc=start_date_search, end_date_utc=end_date_search, selected_service_area=selected_service_area, selected_vehicle_type=selected_vehicle_type)
    data = utc_to_local(data, selected_service_area)
    
    data_load_state.text('Loading booking data...done!')

    data_load_state.text('Loading supply data...')
    start_date_unix_1 = int(start_date.timestamp()*1000)

    supply_days = pd.date_range(start=start_date, end=end_date, normalize=True)
    
    # supply_count_l = []
    # for day in supply_days:
     #   day_unix = int(day.timestamp()*1000)
     #   supply_count_l.append(request_supply_data(headers, dict_service_area[selected_service_area], day_unix, selected_vehicle_type))
    
    # request_supply_data(headers, dict_service_area[selected_service_area], day_unix, selected_vehicle_type)
    # df_supply = pd.DataFrame(data={'date': supply_days, 'supply_count':supply_count_l })
    df_supply = pd.DataFrame(data={'date': supply_days})
    df_supply['supply_count']= request_supply_data(headers, dict_service_area[selected_service_area], start_date_unix_1, selected_vehicle_type)
    
    # output meta 
    print(meta)

    t0 = time.time()
    # create dataframe to plot 
    df_plot = pd.DataFrame(pd.date_range(start=start_date_search, end=end_date,freq='15min'), columns=['date'])
    df_plot['no_bookings']=0
    
    for row in df_plot.index:
        df_plot.at[row, 'no_bookings'] = ( (data['start_date'] <= df_plot.at[row, 'date']) & (data['end_date'] > df_plot.at[row, 'date']) ).sum()
    
    df_plot['no_bookings'] = df_plot['no_bookings'].astype('int')
    df_plot = df_plot.set_index('date')

    data_out = data.loc[ (data['end_date'] > (start_date - pd.Timedelta(minutes=10)) ) & (data['start_date'] <= (end_date + pd.Timedelta(minutes=10)) )].reset_index()
    
    df_plot = pd.merge_asof(df_plot.reset_index(drop=False), df_supply, on="date", direction='nearest')

    df_plot["utilization"] = (df_plot['no_bookings']/df_plot["supply_count"]*100).astype('int')


    print(time.time() - t0)
    if initial_no_vehicles != 0:
        df_plot['delta'] = df_plot['no_bookings'].apply(lambda x: df_plot['no_bookings'][start_date] - x)
        car_col_name = "No of " + selected_vehicle_type
        df_plot[car_col_name] = df_plot['delta'].apply(lambda x: initial_no_vehicles + x)
        cols = ['no_bookings', car_col_name]
        text_subheader = "Number of ongoing bookings and available " + selected_vehicle_type + ", per hour"
    else:
        cols = ['no_bookings', 'supply_count', 'utilization']
        text_subheader = "Number of ongoing " + selected_vehicle_type + " bookings, per hour"


    # create histogram
    st.subheader(text_subheader)
    st.line_chart(df_plot[cols])

    # Show bookings in time period:
    leaving_from_text = "Bookings during Time period in " + selected_service_area
    st.write(data_out)    


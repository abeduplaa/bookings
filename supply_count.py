import requests
import pandas as pd

def request_data(url, params, headers):
    response = requests.get(url, params=params, headers=headers)
    print(response.status_code)
    response.raise_for_status()
    response_data = response.json()['data']
    return response_data


def create_supply_df(start_date, today_date, end_date):
    pass





def request_supply_data(headers, service_area_uuid, start_date, vehicle_class):
    params = {
        'start_date':start_date,
        'end_date': start_date,
        'lot_uuids[]':service_area_uuid,
    }

    if vehicle_class != "All":
        params['vehicle_class'] = vehicle_class

    supply_data = request_data('https://api.drivekyte.com/api/v1/supply_inventories/inventory', params, headers)
    print(supply_data.keys())
    print("Supply count:", supply_data['total']['count'])
    return supply_data['total']['count']
    # call 1x per day for previous days at 7am
    # call 1x for right now 
    # future would be same as right now
    


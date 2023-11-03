""" 
# Goal: Clean/transform/store data in a specific format, to analyse the trend (of EV Fuel components) across the years for the US and Canada
Creator: EPP

"""


import requests as r
import pandas as pd
import datetime as dt
import dateutil.relativedelta
import re
import os
from dotenv import load_dotenv
import connector


load_dotenv()


# Note: I've uploadet .env for education purposes. Otherwise it would be in .gitignore
API_KEY = os.getenv('API_KEY')
fuel_type = 'all'
state = "all"
access_type = 'all'



def get_data(url_complaints):
    """
    Retrieves data from a given API URL using the 'requests' library.

    Parameters:
    url_complaints (str): The URL of the API endpoint containing complaint data.

    Returns:
    pandas.DataFrame: A DataFrame containing the complaint data extracted from the API.

    This function sends an HTTP GET request to the specified URL, retrieves the data, and converts it into a pandas DataFrame.
    It handles potential HTTP exceptions and raises an error if the request is unsuccessful.
    """
    # Catch HTTP (400, 500, etc..) exceptions that may occur
    try:
            resp_alt_fuel_stations = r.get(url_alt_fuel_stations)
            resp_alt_fuel_stations.raise_for_status()
            return pd.DataFrame(resp_alt_fuel_stations.json()['fuel_stations'])

    except r.exceptions.RequestException as e:
            raise SystemExit(e)




def process_data(alt_fuel_stations_df):
    """
    Process and clean a dataset of alternative fuel stations for analysis.

    Parameters:
    alt_fuel_stations_df (pandas.DataFrame): The dataset containing information about alternative fuel stations.

    Returns:
    pandas.DataFrame: A cleaned and processed DataFrame ready for analysis.

    This function performs various data processing and cleaning operations on the input DataFrame, including dropping deprecated columns,
         removing columns with more than 50% null values, dropping duplicate rows, filling missing values, and standardizing data formats.
    The resulting DataFrame is tailored for analysis of electric vehicle charging stations.
    """
    deprecated_columns = ["groups_with_access_code", "ng_fill_type_code", "ng_psi", "ng_vehicle_class", "cng_vehicle_class", "lng_vehicle_class", "groups_with_access_code_fr"]
    alt_fuel_stations_df = alt_fuel_stations_df.drop(deprecated_columns, axis=1)


    # If a column has more than 50% of its values as null then I'll remove it. There won't be enought data to (later) extract any conclusions
    alt_fuel_stations_df = alt_fuel_stations_df.drop(alt_fuel_stations_df.columns[alt_fuel_stations_df.isnull().mean() > 0.5], axis=1)

    # Drop any duplicated rows (if they exist)
    alt_fuel_stations_df = alt_fuel_stations_df.loc[alt_fuel_stations_df.astype(str).drop_duplicates().index]

    # Filling missing values
    alt_fuel_stations_df["access_days_time"] = alt_fuel_stations_df["access_days_time"].fillna("N/D")

    # Adding new row to be able to make monthly/yearly (snapshot) historic comparisons
    alt_fuel_stations_df["monthYearHistory"] = dt.date.today()

    # Making sure these states/Provinces have the right corresponding country Canada
    canada_provinces = ["ON", "QC", "NS", "NB", "MB", "BC", "PE", "SK", "AB", "NL"]
    alt_fuel_stations_df.loc[alt_fuel_stations_df['state'].isin(canada_provinces), 'country'] = 'CA'

    # Filling NaN values ('city') based on other rows with filled information
    states_teste = alt_fuel_stations_df[["city", "state"]].copy()
    states_teste = states_teste.drop_duplicates(subset=['city'])
    alt_fuel_stations_df = pd.merge(alt_fuel_stations_df, states_teste, on="city", how="left")

    # removing values outside US/CA info (e.g., Bangalore - India)
    alt_fuel_stations_df = alt_fuel_stations_df.drop("state_x", axis=1)
    alt_fuel_stations_df = alt_fuel_stations_df.rename({'state_y': 'state'}, axis=1)
    alt_fuel_stations_df = alt_fuel_stations_df[alt_fuel_stations_df[['state','access_code']].notna().all(1)]


    # If a city contains a number then it's a street. We don't want those
    alt_fuel_stations_df = alt_fuel_stations_df[~alt_fuel_stations_df['city'].str.contains('\d', regex = True)]


    #  Filling Nan Date values and casting types
    alt_fuel_stations_df[['date_last_confirmed','open_date', 'updated_at']] = alt_fuel_stations_df[['date_last_confirmed','open_date', 'updated_at']].fillna('1900-01-01')
    alt_fuel_stations_df[['date_last_confirmed','open_date', 'updated_at']] = alt_fuel_stations_df[['date_last_confirmed','open_date', 'updated_at']].apply(pd.to_datetime, format = '%Y-%m-%d')
    # removing Timezones
    alt_fuel_stations_df['updated_at'] = alt_fuel_stations_df['updated_at'].dt.tz_localize(None)
    
    alt_fuel_stations_df["ev_level2_evse_num"] = alt_fuel_stations_df["ev_level2_evse_num"].fillna(0)
    alt_fuel_stations_df["ev_level2_evse_num"] = alt_fuel_stations_df["ev_level2_evse_num"].astype(str).str.replace(".0","", regex=False)
    alt_fuel_stations_df[['id', 'ev_level2_evse_num']] = alt_fuel_stations_df[['id', 'ev_level2_evse_num']].apply(pd.to_numeric)


    # Replacing values
    alt_fuel_stations_df['ev_workplace_charging'] = alt_fuel_stations_df['ev_workplace_charging'].apply(lambda x: False if x is None else x)
    alt_fuel_stations_df["ev_workplace_charging"] = alt_fuel_stations_df["ev_workplace_charging"].astype(bool)

    # Replacing values &  Formatting URLs
    alt_fuel_stations_df["ev_network_web"] = alt_fuel_stations_df["ev_network_web"].str.split("/").str[2]
    alt_fuel_stations_df[["ev_network", "ev_network_web"]] = alt_fuel_stations_df[["ev_network", "ev_network_web"]].fillna("N/D")
    alt_fuel_stations_df["ev_network_web"] = alt_fuel_stations_df["ev_network_web"].apply(lambda x: f'www.{x}' if ('www.' not in x and x!="N/D") else x)

    # Formatting phone number column
    alt_fuel_stations_df["station_phone"] = alt_fuel_stations_df["station_phone"].str.replace('[^a-zA-Z0-9]','', regex=True)
    alt_fuel_stations_df['station_phone'] = alt_fuel_stations_df['station_phone'].apply(lambda x: '0000000000' if x is None else x)
    alt_fuel_stations_df["station_phone"] = alt_fuel_stations_df["station_phone"].apply(lambda x: f'{x[0:3]}-{x[3:6]}-{x[6:10]}')


    # Removing outliers (based on the 75% qntl)
    quantile = alt_fuel_stations_df["ev_level2_evse_num"].quantile(0.75)
    alt_fuel_stations_df = alt_fuel_stations_df[alt_fuel_stations_df["ev_level2_evse_num"] < quantile]

    # Split each connector type in to a new row
    alt_fuel_stations_df = alt_fuel_stations_df.explode("ev_connector_types")

    # For simplicity purposes, I'll filter just the Electrics
    alt_fuel_stations_df = alt_fuel_stations_df[alt_fuel_stations_df["fuel_type_code"] == "ELEC"]

    return alt_fuel_stations_df





def transform_data(alt_fuel_stations_df):
    """
    Transforms a dataset of alternative fuel stations for connector type analysis.

    Parameters:
    alt_fuel_stations_df (pandas.DataFrame): The dataset containing information about alternative fuel stations.

    Returns:
    pandas.DataFrame: A DataFrame with aggregated data for connector types analysis.

    This function takes the input DataFrame and aggregates the data to analyze the number of different connector types available in alternative fuel stations.
    The resulting DataFrame is grouped by various attributes such as monthYearHistory, fuel_type_code, country, state, city, and ev_connector_types.
    It counts the number of different connectors and presents the information in a format suitable for connector type analysis.

    Example:
    >> connector_data = transform_data(alt_fuel_stations_df)
    >> print(connector_data)
    """
    return alt_fuel_stations_df.groupby(["monthYearHistory", "fuel_type_code", "country", "state", "city", "ev_connector_types"]).size().to_frame("numDifConnectors").reset_index()





if __name__ == "__main__":

    try:
        print("Executing script...")

        url_alt_fuel_stations  = f"https://developer.nrel.gov/api/alt-fuel-stations/v1.json?api_key={API_KEY}&access={access_type}&fuel_type={fuel_type}&state={state}"
        alt_fuel_stations_df = get_data(url_alt_fuel_stations)

        proc_alt_fuel_stations_df = process_data(alt_fuel_stations_df)
        transf_alt_fuel_df = transform_data(proc_alt_fuel_stations_df)
        
        # Saves data to csv
        proc_alt_fuel_stations_df.to_csv(r"datasets\alternative\proc_alt_fuel_stations_df.csv", index = False)
        transf_alt_fuel_df.to_csv(r"datasets\alternative\transf_alt_fuel_df.csv", index = False)
        
        # Uploads data to the database
        connector.main(transf_alt_fuel_df, "transf_alt_fuel_df")

        print("** Finished **")


    except:
        print("Something went wrong")


    # print(proc_alt_fuel_stations_df)
    # print(transf_alt_fuel_df)

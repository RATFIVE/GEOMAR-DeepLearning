
from utils.Database import Database
from utils.Copernicus import AdvancedCopernicus
from utils.OpenMeteoWeather import OpenMeteoWeather
from utils.PlanetPositions import PlanetPositions
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import json


# ------------ Initialize the global Variables ------------

# Define Absolute maximum and minimum values for date and location
ABSOLUTE_END_DATE:str = datetime.datetime.now().isoformat().split("T")[0]

ABSOLUTE_MINIMUM_LONGITUDE:float = 9.041532516479492
ABSOLUTE_MAXIMUM_LONGITUDE:float = 30.208656311035156
ABSOLUTE_MINIMUM_LATIDUDE:float = 53.00829315185547
ABSOLUTE_MAXIMUM_LATIDUDE:float = 65.89141845703125

START_DATE:str = "2025-01-01"
END_DATE:str = "2025-02-1"

MINIMUM_LONGITUDE:float = 9.59
MAXIMUM_LONGITUDE:float = 12.037
MINIMUM_LATIDUDE:float = 55.486
MAXIMUM_LATIDUDE:float = 55.616

# For Testing
MINIMUM_LONGITUDE=10.038345850696412
MAXIMUM_LONGITUDE=10.365962458698567
MINIMUM_LATIDUDE=54.27381478077755
MAXIMUM_LATIDUDE=54.52976525577923

OUTPUT_FILENAME:str = "output.nc"

DB_URL = 'localhost'
#DB_URL = 'host.docker.internal'
DB_NAME = 'deep-learning'
#DB_COLLECTION = 'ozean-weather-data'
DB_COLLECTION = 'test'




# ------------ Initialize the classes ------------
AdvancedCopernicus = AdvancedCopernicus()


# ------------ Helper Functions ------------

def process_dataframe(df:pd.DataFrame) -> pd.DataFrame:
    for column in df.select_dtypes(include=["float"]).columns:
        df[column] = df[column].astype(np.float32)  # Konvertiere alle Float-Typen zu float32
    df["latitude"] = df["latitude"].astype(np.float32).round(6)
    df["longitude"] = df["longitude"].astype(np.float32).round(6)
    df['time'] = pd.to_datetime(df['time']).dt.tz_localize(None).dt.round('h')

    return df


# ------------ Get data from AdvancedCopernicus ------------
print("\nGetting data from AdvancedCopernicus...\n")
copernicus_data = AdvancedCopernicus.get_subset(
                dataset_id="cmems_mod_bal_phy_anfc_PT1H-i",
                dataset_version="202411",
                variables=["bottomT", "mlotst", "siconc", "sithick", "sla", "so", "sob", "thetao", "uo", "vo", "wo"], 
                minimum_longitude=MINIMUM_LONGITUDE,
                maximum_longitude=MAXIMUM_LONGITUDE,
                minimum_latitude=MINIMUM_LATIDUDE,
                maximum_latitude=MAXIMUM_LATIDUDE,
                start_datetime=START_DATE,
                end_datetime=END_DATE,
                minimum_depth=0.5016462206840515,
                maximum_depth=0.5016462206840515,
                coordinates_selection_method="strict-inside",
                disable_progress_bar=False,
                output_filename=OUTPUT_FILENAME
                )
            


df_copernicus = copernicus_data.to_dataframe().reset_index()
# put column time in front
df_copernicus = df_copernicus[["time"] + [col for col in df_copernicus.columns if col != "time"]]

df_copernicus.dropna(axis=0, 
                     subset=["bottomT", "mlotst", "siconc", "sithick", "sla", "so", "sob", "thetao", "uo", "vo", "wo"], 
                     inplace=True,
                     how="all")

df_copernicus = process_dataframe(df_copernicus)

db = Database(
    db_url=DB_URL,
    db_name=DB_NAME,
    collection_name=DB_COLLECTION
    )
    

db_data_all = db.get_all_data(key="time")
db.close_connection()

if db_data_all:
    df_db = pd.DataFrame(db_data_all).drop(columns=['_id']).loc[:, ['time', 'latitude', 'longitude']]

    df_db = process_dataframe(df_db)

    # Filtere Zeilen, die in df_db existieren
    db_tuples = set(zip(df_db["time"], df_db["latitude"], df_db["longitude"]))
    df_copernicus = df_copernicus[~df_copernicus.apply(lambda row: (row["time"], row["latitude"], row["longitude"]) in db_tuples, axis=1)]
    print(f'Reduced data {len(df_copernicus)}')


# Helper Function
def upload_article_if_new(db_data, not_db_data):
    # Check if the article is already in the database
    for doc in db_data:
        if (doc.get('time') == not_db_data.get('time')) and (doc.get('latitude') == not_db_data.get('latitude')) and (doc.get('longitude') == not_db_data.get('longitude')):
            #print('Data already in the database, skipping upload...\n')
            return False
        
    return True


print("\nParsing data to upload to Database...\n")
for idx, (time, latitude, longitude) in enumerate(tqdm(zip(df_copernicus['time'], df_copernicus['latitude'], df_copernicus['longitude']), desc='Processing data', total=len(df_copernicus))):

    # bring time to isoformat
    time = time.isoformat().split('T')[0]
    
    open_meteo_weather = OpenMeteoWeather(
    latitude=[latitude],
    longitude=[longitude],
    start_date=time,
    end_date=time
    ) 

    df_openweather = open_meteo_weather.get_weather_dataframe().rename(columns={"date": "time"})
    df_openweather['time'] = df_openweather['time'].dt.tz_localize(None) # convert datetime64[ns, UTC] to datetime64[ns]
    df_openweather['latitude'] = latitude
    df_openweather['longitude'] = longitude
    
    df_merged = pd.merge(df_copernicus, df_openweather, on=['time', 'latitude', 'longitude'], how='inner')
    df_merged = process_dataframe(df_merged)
    print(df_merged.info())
    print(df_merged.head())
    #break

    # upload to database
    df_json = df_merged.to_json(orient='records')
    df_json = json.loads(df_json)

    db = Database(
        db_url=DB_URL,
        db_name=DB_NAME,
        collection_name=DB_COLLECTION
        )
    upload_list = []
    for item in df_json:
        item["time"] = pd.to_datetime(item["time"], unit='ms')

        db_data_all = db.get_all_data(key="time")
        if upload_article_if_new(db_data_all, item) == False:
            continue
        upload_list.append(item)

    db.upload_many(upload_list)
    db.close_connection()

print("Data uploaded to Database successfully!\n")
print("Finished!\n")




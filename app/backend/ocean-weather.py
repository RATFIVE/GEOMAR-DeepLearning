from utils.Database import Database
from utils.Copernicus import AdvancedCopernicus
from utils.OpenMeteoWeather import OpenMeteoWeather
import pandas as pd
import numpy as np
import datetime
import json
from tqdm import tqdm

# ------------ Initialize Global Variables ------------
ABSOLUTE_END_DATE = datetime.datetime.now().strftime("%Y-%m-%d")

START_DATE = "2025-01-01"
END_DATE = "2025-02-01"

# Define bounding box for data retrieval
BBOX = {
    "min_lon": 10.038345850696412,
    "max_lon": 10.365962458698567,
    "min_lat": 54.27381478077755,
    "max_lat": 54.52976525577923
}

OUTPUT_FILENAME = "output.nc"

DB_CONFIG = {
    "url": "localhost",
    "name": "deep-learning",
    "collection": "test"
}

# ------------ Helper Functions ------------
def process_dataframe(df: pd.DataFrame, convert_time: bool = False) -> pd.DataFrame:
    """Converts float columns to float32 and rounds latitude/longitude for consistency."""
    float_cols = df.select_dtypes(include=["float"]).columns
    df[float_cols] = df[float_cols].astype(np.float32)

    df["latitude"] = df["latitude"].astype(np.float32).round(6)
    df["longitude"] = df["longitude"].astype(np.float32).round(6)

    if convert_time and not np.issubdtype(df['time'].dtype, np.datetime64):
        df["time"] = pd.to_datetime(df["time"])

    return df

# ------------ Fetch Data from AdvancedCopernicus ------------
print("\nFetching data from AdvancedCopernicus...\n")
copernicus = AdvancedCopernicus()
copernicus_data = copernicus.get_subset(
    dataset_id="cmems_mod_bal_phy_anfc_PT1H-i",
    dataset_version="202411",
    variables=["bottomT", "mlotst", "siconc", "sithick", "sla", "so", "sob", "thetao", "uo", "vo", "wo"],
    minimum_longitude=BBOX["min_lon"],
    maximum_longitude=BBOX["max_lon"],
    minimum_latitude=BBOX["min_lat"],
    maximum_latitude=BBOX["max_lat"],
    start_datetime=START_DATE,
    end_datetime=END_DATE,
    minimum_depth=0.5016462206840515,
    maximum_depth=0.5016462206840515,
    coordinates_selection_method="strict-inside",
    disable_progress_bar=False,
    output_filename=OUTPUT_FILENAME
)

df_copernicus = copernicus_data.to_dataframe().reset_index()
df_copernicus = df_copernicus[["time"] + [col for col in df_copernicus.columns if col != "time"]]

# Remove rows where all key variables are NaN
key_vars = ["bottomT", "mlotst", "siconc", "sithick", "sla", "so", "sob", "thetao", "uo", "vo", "wo"]
df_copernicus.dropna(subset=key_vars, how="all", inplace=True, axis=0)
df_copernicus = process_dataframe(df_copernicus, convert_time=True)

# ------------ Fetch Existing Data from Database ------------
db = Database(db_url=DB_CONFIG["url"], db_name=DB_CONFIG["name"], collection_name=DB_CONFIG["collection"])
db_data_all = db.get_all_data(key="time")
db.close_connection()

if db_data_all:
    df_db = pd.DataFrame(db_data_all).drop(columns=["_id"])[["time", "latitude", "longitude"]]
    df_db = process_dataframe(df_db, convert_time=True)
    len_before = len(df_copernicus)
    # Use a performant merge operation instead of looping
    df_copernicus = df_copernicus.merge(df_db, on=["time", "latitude", "longitude"], how="left", indicator=True)
    df_copernicus = df_copernicus[df_copernicus["_merge"] == "left_only"].drop(columns=["_merge"])
    len_after = len(df_copernicus)
    print(f"\nRemoved {len_before - len_after} existing records from the Copernicus data")
    print(f"Reduced data: {len(df_copernicus)} rows\n")

# ------------ Fetch OpenMeteoWeather Data and Upload ------------
unique_times = df_copernicus["time"].unique()

# Extract unique latitude-longitude pairs
lat_lon_list = sorted(set(zip(df_copernicus["latitude"], df_copernicus["longitude"])))
latitudes, longitudes = zip(*lat_lon_list)

print(f"\nUnique locations: {len(lat_lon_list)}, Unique times: {len(unique_times)}\n")

db = Database(db_url=DB_CONFIG["url"], db_name=DB_CONFIG["name"], collection_name=DB_CONFIG["collection"])

for i in tqdm(range(0, len(unique_times), 10), desc="Uploading data to the database", total=len(unique_times) // 10):
    time = unique_times[i]
    time_str = time.strftime("%Y-%m-%d")

    lat_subset = latitudes[:i + 10]
    lon_subset = longitudes[:i + 10]

    

    open_meteo_weather = OpenMeteoWeather(
        latitudes=lat_subset,
        longitudes=lon_subset,
        start_date=time_str,
        end_date=time_str
    )
    df_openweather = open_meteo_weather.get_weather_dataframe()
    df_openweather = df_openweather[["time"] + [col for col in df_openweather.columns if col != "time"]]
    df_openweather = process_dataframe(df_openweather, convert_time=True)

    df_merged = pd.merge(df_copernicus, df_openweather, on=["time", "latitude", "longitude"], how="inner")

    if not df_merged.empty:
        db.upload_many(df_merged.to_dict(orient="records"))
        print(f"Uploaded {len(df_merged)} records to the database")
    #break
db.close_connection()

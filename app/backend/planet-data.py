
from utils.Database import Database
from utils.PlanetPositions import PlanetPositions
import pandas as pd
import numpy as np
import datetime
from tqdm import tqdm
import json


# Define Absolute maximum and minimum values for date and location
ABSOLUTE_END_DATE:str = datetime.datetime.now().isoformat().split("T")[0]


START_DATE:str = "2025-01-01"
END_DATE:str = "2025-02-1"

DB_URL = 'localhost'
#DB_URL = 'host.docker.internal'
DB_NAME = 'deep-learning'
DB_COLLECTION = 'planet-data'



# ------------ Helper Functions ------------

def process_dataframe(df:pd.DataFrame) -> pd.DataFrame:
    for column in df.select_dtypes(include=["float"]).columns:
        df[column] = df[column].astype(np.float32)  # Konvertiere alle Float-Typen zu float32
    df['time'] = pd.to_datetime(df['time'], unit='s', format='ISO8601').dt.tz_localize(None).dt.round('h')
# df['time'] = pd.to_datetime(df['time'])

# # Runde auf die nächste Stunde
# df['time'] = df['time'].dt.round('h')
    return df



print("\nGetting data from PlanetPositions...\n")
pp = PlanetPositions(start_date=START_DATE, stop_date=END_DATE, step='1h')
pp.fetch_data()
pp.convert_time()
df_planet = pp.get_dataframe()





# convert colum datetime to YYYY-MM-DD HH:MM:SS
#df_planet['datetime'] = df_planet['datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
df_planet = df_planet.drop(columns=['datetime_str', 'planet']).rename(columns={'datetime':'time', 'targetname':'planet'})
# put time column to the first position
df_planet = df_planet[['time'] + [col for col in df_planet.columns if col != 'time']]

df_planet = process_dataframe(df_planet)



db = Database(
    db_url=DB_URL,
    db_name=DB_NAME,
    collection_name=DB_COLLECTION
    )
    

db_data_all = db.get_all_data(key="time")
db.close_connection()

if db_data_all:
    df_db = pd.DataFrame(db_data_all).drop(columns=['_id']).loc[:, ['time', 'planet']]

    df_db = process_dataframe(df_db)

    # Filtere Zeilen, die in df_db existieren
    db_tuples = set(zip(df_db["time"], df_db["planet"]))
    df_planet = df_planet[~df_planet.apply(lambda row: (row["time"], row["planet"]) in db_tuples, axis=1)]




# Helper Function
def upload_article_if_new(db_data, not_db_data):
    # Check if the article is already in the database
    for doc in db_data:
        if (doc.get('time') == not_db_data.get('time')) and (doc.get('planet') == not_db_data.get('planet')):
            #print('Data already in the database, skipping upload...\n')
            return False
        
    return True


print("\nParsing data to upload to Database...\n")
db = Database(
    db_url=DB_URL,
    db_name=DB_NAME,
    collection_name=DB_COLLECTION
    )
# upload to database
df_json = df_planet.to_json(orient='records')
df_json = json.loads(df_json)
upload_list = []
for item in tqdm(df_json, desc="Uploading data to Database", total=len(df_json)):
    item["time"] = pd.to_datetime(item["time"], unit='ms')




    # db_data_all = db.get_all_data(key="time")
    # if upload_article_if_new(db_data_all, item) == False:
    #     continue
    upload_list.append(item)
    # db.upload_one(item)
    #print(f"Data for {item['planet']} at {item['time']} uploaded to Database successfully!\n")

db.upload_many(upload_list)
db.close_connection()

print("Data uploaded to Database successfully!\n")
print("Finished!\n")



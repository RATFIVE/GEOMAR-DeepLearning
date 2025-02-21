import fastapi
import uvicorn

from utils.Copernicus import AdvancedCopernicus
from utils.OpenMeteoWeather import OpenMeteoWeather
from utils.PlanetPositions import PlanetPositions

# ------------ Initialize the global Variables ------------

START_DATE:str = "2024-01-01"
END_DATE:str = "2025-02-1"

TOP_LEFT_LAT:float = 55.616
TOP_LEFT_LON:float = 9.59

BOTTOM_RIGHT_LAT:float = 55.486
BOTTOM_RIGHT_LON:float = 12.037

OUTPUT_FILENAME:str = "output.nc"




# ------------ Initialize the classes ------------
AdvancedCopernicus = AdvancedCopernicus()
#OpenMeteoWeather = OpenMeteoWeather()
#PlanetPositions = PlanetPositions()

# ------------ Get data from AdvancedCopernicus ------------
copernicus_data = AdvancedCopernicus.get_subset(
                dataset_id="cmems_mod_glo_phy_anfc_0.083deg_PT1H-m",
                dataset_version="202406",
                variables=["so", "thetao", "vo", "zos", "uo"], 
                minimum_longitude=TOP_LEFT_LON,
                maximum_longitude=BOTTOM_RIGHT_LON,
                minimum_latitude=BOTTOM_RIGHT_LAT,
                maximum_latitude=TOP_LEFT_LAT,
                start_datetime=START_DATE,
                end_datetime=END_DATE,
                minimum_depth=0.49402499198913574,
                maximum_depth=0.49402499198913574,
                coordinates_selection_method="strict-inside",
                disable_progress_bar=False,
                output_filename=OUTPUT_FILENAME
                )
            



print(copernicus_data.info())

# ------------ Bring data to the frontend ------------
app = fastapi.FastAPI()
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime, timedelta
from tqdm import tqdm


class OpenMeteoWeather:
    def __init__(self, latitudes:list, longitudes:list, start_date:str, end_date:str):
        # Initialize parameters
        self.latitudes = latitudes
        self.longitudes = longitudes
        self.start_date = start_date
        self.end_date = end_date

        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=60.0, 
                              status_to_retry=(500, 502, 503, 504, 400, 429))
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
    
    def fetch_weather_data(self):
        all_data = []
        today = datetime.today().date()
        start_date = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        
        for lat, lon in zip(self.latitudes, self.longitudes):
            params = {
                "latitude": lat,
                "longitude": lon,
                "hourly": [
                    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", 
                    "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth",
                    "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", 
                    "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", 
                    "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_80m", 
                    "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", 
                    "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", 
                    "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", 
                    "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", 
                    "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", 
                    "soil_moisture_27_to_81cm"
                ],
                "timezone": "Europe/Berlin",
            }
            
            if start_date < today:
                archive_params = params.copy()
                archive_params["start_date"] = self.start_date
                archive_params["end_date"] = min(end_date, today - timedelta(days=1))
                archive_url = "https://archive-api.open-meteo.com/v1/archive"
                archive_response = self.openmeteo.weather_api(archive_url, params=archive_params)
                all_data.extend([(lat, lon, r) for r in archive_response])
                
            if end_date >= today:
                forecast_params = params.copy()
                forecast_params["start_date"] = max(self.start_date, str(today))
                forecast_params["end_date"] = self.end_date
                forecast_url = "https://api.open-meteo.com/v1/forecast"
                forecast_response = self.openmeteo.weather_api(forecast_url, params=forecast_params)
                all_data.extend([(lat, lon, r) for r in forecast_response])

        return self.process_weather_data(all_data)
    
    def process_weather_data(self, data_responses):
        data_frames = []
        
        for lat, lon, response in tqdm(data_responses, desc="Processing weather data", total=len(data_responses)):
            hourly = response.Hourly()
            hourly_data = {
                "latitude": lat,
                "longitude": lon,
                "time": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=False),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=False),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                )
            }
            
            variables = [
                "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", 
                "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth",
                "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", 
                "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", 
                "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_80m", 
                "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", 
                "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", 
                "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", 
                "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", 
                "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", 
                "soil_moisture_27_to_81cm"
            ]
            
            for idx, var in enumerate(variables):
                hourly_data[var] = hourly.Variables(idx).ValuesAsNumpy()
                
            data_frames.append(pd.DataFrame(hourly_data))
        
        return pd.concat(data_frames, ignore_index=True)
    
    def get_weather_dataframe(self):
        return self.fetch_weather_data()


if __name__ == "__main__":
    from datetime import datetime
    
    end_date = datetime.today().strftime("%Y-%m-%d")
    weather = OpenMeteoWeather(latitudes=[54.0, 55.0], 
                               longitudes=[10.0, 11.0], 
                               start_date="2025-02-01", 
                               end_date=end_date)
    df = weather.get_weather_dataframe()
    print(df)

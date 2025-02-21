import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime, timedelta


class OpenMeteoWeather:
    def __init__(self, latitude, longitude, start_date, end_date):
        # Initialize parameters
        self.latitude = latitude
        self.longitude = longitude
        self.start_date = start_date
        self.end_date = end_date

        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=1.0)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)

    # def fetch_weather_data(self, archive=False):
    #     # Define the URL and parameters
    #     if archive:
    #         url="https://archive-api.open-meteo.com/v1/archive"
    #     else:
    #         url = "https://api.open-meteo.com/v1/forecast"
    #     params = {
    #         "latitude": self.latitude,
    #         "longitude": self.longitude,
    #         "hourly": [
    #             "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", 
    #             "precipitation_probability", "precipitation", "rain", "showers", "snowfall", "snow_depth",
    #             "weather_code", "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", 
    #             "cloud_cover_mid", "cloud_cover_high", "visibility", "evapotranspiration", 
    #             "et0_fao_evapotranspiration", "vapour_pressure_deficit", "wind_speed_10m", "wind_speed_80m", 
    #             "wind_speed_120m", "wind_speed_180m", "wind_direction_10m", "wind_direction_80m", 
    #             "wind_direction_120m", "wind_direction_180m", "wind_gusts_10m", "temperature_80m", 
    #             "temperature_120m", "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm", 
    #             "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm", 
    #             "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm", 
    #             "soil_moisture_27_to_81cm"
    #         ],
    #         "timezone": "Europe/Berlin",
    #         "start_date": self.start_date,
    #         "end_date": self.end_date
    #     }
        
    #     # Fetch data from Open-Meteo API
    #     responses = self.openmeteo.weather_api(url, params=params)
    #     return responses[0]

    def fetch_weather_data(self):
        # Define base parameters
        params = {
            "latitude": ",".join(str(lat) for lat in self.latitude),
            "longitude": ",".join(str(lon) for lon in self.longitude),
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
            "timezone": "Europe/Berlin"
        }

        # Convert start_date to a datetime object
        today = datetime.today().date()
        start_date = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(self.end_date, "%Y-%m-%d").date()

        # Initialize empty data storage
        all_data = []

        # Fetch archive data if needed
        if start_date < today:
            archive_params = params.copy()
            archive_params["start_date"] = self.start_date
            archive_params["end_date"] = min(end_date, today - timedelta(days=1))  # Archive only available for past days
            archive_url = "https://archive-api.open-meteo.com/v1/archive"
            
            archive_response = self.openmeteo.weather_api(archive_url, params=archive_params)
            all_data.append(archive_response[0])  # Store historical data

        # Fetch forecast data if needed
        if end_date >= today:
            forecast_params = params.copy()
            forecast_params["start_date"] = max(self.start_date, str(today))  # Forecast only for today onward
            forecast_params["end_date"] = self.end_date
            forecast_url = "https://api.open-meteo.com/v1/forecast"
            
            forecast_response = self.openmeteo.weather_api(forecast_url, params=forecast_params)
            all_data.append(forecast_response[0])  # Store forecast data

        # Merge historical and forecast data
        if len(all_data) == 2:
            merged_data = self.merge_weather_data(all_data[0], all_data[1])
        else:
            merged_data = self.process_weather_data(all_data[0])
            #merged_data = all_data[0]

        return merged_data

    def merge_weather_data(self, archive_data, forecast_data):
        """Merge historical and forecast data"""
        archive_data = self.process_weather_data(archive_data)
        forecast_data = self.process_weather_data(forecast_data)

        
        merged = pd.concat([archive_data, forecast_data]).sort_values("date", ascending=False).reset_index(drop=True)
        return merged

    def process_weather_data(self, response):
        # Process hourly data
        hourly = response.Hourly()
        hourly_data = {"date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}

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
        
        # Collecting each variable's data
        for idx, var in enumerate(variables):
            hourly_data[var] = hourly.Variables(idx).ValuesAsNumpy()

        # Convert to pandas DataFrame
        hourly_dataframe = pd.DataFrame(data=hourly_data)
        return hourly_dataframe

    def get_weather_dataframe(self):
        # Fetch and process weather data
        response = self.fetch_weather_data()
        #return self.process_weather_data(response)
        return response


if __name__ == "__main__":
    import time
    from datetime import datetime

    end_datetime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # Extrahiere nur das Datum
    end_date = datetime.strptime(end_datetime.split(" ")[0], "%Y-%m-%d").date()
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date_str =end_date_str.split("T")[0]
    # Example usage:
    weather = OpenMeteoWeather(latitude=54.32, 
                               longitude=10.12, 
                               start_date="2025-02-01", 
                               end_date=end_date_str
                               )
    df = weather.get_weather_dataframe()
    print(df)

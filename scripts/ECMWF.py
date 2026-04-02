import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from sqlalchemy import create_engine
import os
import sys

# Ensure db_config (in the same directory) is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
# from dotenv import load_dotenv

# load_dotenv(dotenv_path='.env')

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 51.2205,
	"longitude": 4.4003,
	"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain", "showers", "snowfall", "runoff", "visibility", "weather_code", "surface_pressure", "pressure_msl", "cloud_cover_low", "cloud_cover", "cloud_cover_mid", "cloud_cover_high", "sunshine_duration", "et0_fao_evapotranspiration", "potential_evapotranspiration", "wind_speed_10m", "wind_speed_100m", "wind_speed_200m", "wind_direction_10m", "wind_direction_100m", "wind_direction_200m", "wind_gusts_10m", "cape", "convective_inhibition", "total_column_integrated_water_vapour", "surface_temperature", "vapour_pressure_deficit", "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm", "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm", "is_day", "temperature_2m_min", "wet_bulb_temperature_2m", "temperature_2m_max", "precipitation_type", "sea_level_height_msl", "ocean_current_velocity", "ocean_current_direction", "lightning_density", "sea_ice_thickness", "roughness_length", "albedo", "k_index", "snowfall_water_equivalent", "snow_depth_water_equivalent", "shortwave_radiation", "direct_radiation", "diffuse_radiation", "terrestrial_radiation", "global_tilted_irradiance", "direct_normal_irradiance", "shortwave_radiation_instant", "terrestrial_radiation_instant", "global_tilted_irradiance_instant", "direct_normal_irradiance_instant", "direct_radiation_instant", "diffuse_radiation_instant", "temperature_1000hPa", "temperature_925hPa", "temperature_850hPa", "temperature_700hPa", "temperature_600hPa", "temperature_500hPa", "temperature_400hPa", "temperature_250hPa", "temperature_300hPa", "temperature_200hPa", "temperature_150hPa", "temperature_100hPa", "temperature_50hPa", "relative_humidity_1000hPa", "relative_humidity_925hPa", "relative_humidity_850hPa", "relative_humidity_700hPa", "relative_humidity_600hPa", "relative_humidity_500hPa", "relative_humidity_400hPa", "relative_humidity_300hPa", "relative_humidity_200hPa", "relative_humidity_250hPa", "relative_humidity_150hPa", "relative_humidity_100hPa", "relative_humidity_50hPa", "cloud_cover_1000hPa", "cloud_cover_925hPa", "cloud_cover_700hPa", "cloud_cover_850hPa", "cloud_cover_600hPa", "cloud_cover_500hPa", "cloud_cover_400hPa", "cloud_cover_300hPa", "cloud_cover_250hPa", "cloud_cover_200hPa", "cloud_cover_150hPa", "cloud_cover_100hPa", "cloud_cover_50hPa", "wind_speed_1000hPa", "wind_speed_925hPa", "wind_speed_850hPa", "wind_speed_700hPa", "wind_speed_600hPa", "wind_speed_500hPa", "wind_speed_400hPa", "wind_speed_300hPa", "wind_speed_200hPa", "wind_speed_250hPa", "wind_speed_150hPa", "wind_speed_100hPa", "wind_speed_50hPa", "wind_direction_1000hPa", "wind_direction_925hPa", "wind_direction_600hPa", "wind_direction_850hPa", "wind_direction_700hPa", "wind_direction_500hPa", "wind_direction_400hPa", "wind_direction_300hPa", "wind_direction_200hPa", "wind_direction_250hPa", "wind_direction_150hPa", "wind_direction_100hPa", "wind_direction_50hPa", "vertical_velocity_1000hPa", "vertical_velocity_925hPa", "vertical_velocity_850hPa", "vertical_velocity_700hPa", "vertical_velocity_600hPa", "vertical_velocity_500hPa", "vertical_velocity_400hPa", "vertical_velocity_300hPa", "vertical_velocity_250hPa", "vertical_velocity_200hPa", "vertical_velocity_150hPa", "vertical_velocity_100hPa", "vertical_velocity_50hPa", "geopotential_height_1000hPa", "geopotential_height_925hPa", "geopotential_height_850hPa", "geopotential_height_700hPa", "geopotential_height_600hPa", "geopotential_height_500hPa", "geopotential_height_300hPa", "geopotential_height_400hPa", "geopotential_height_250hPa", "geopotential_height_200hPa", "geopotential_height_150hPa", "geopotential_height_100hPa", "geopotential_height_50hPa"],
	"models": "ecmwf_ifs",
	"wind_speed_unit": "ms",
}
responses = openmeteo.weather_api(url, params=params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]

print(response)
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
hourly_rain = hourly.Variables(5).ValuesAsNumpy()
hourly_showers = hourly.Variables(6).ValuesAsNumpy()
hourly_snowfall = hourly.Variables(7).ValuesAsNumpy()
hourly_runoff = hourly.Variables(8).ValuesAsNumpy()
hourly_visibility = hourly.Variables(9).ValuesAsNumpy()
hourly_weather_code = hourly.Variables(10).ValuesAsNumpy()
hourly_surface_pressure = hourly.Variables(11).ValuesAsNumpy()
hourly_pressure_msl = hourly.Variables(12).ValuesAsNumpy()
hourly_cloud_cover_low = hourly.Variables(13).ValuesAsNumpy()
hourly_cloud_cover = hourly.Variables(14).ValuesAsNumpy()
hourly_cloud_cover_mid = hourly.Variables(15).ValuesAsNumpy()
hourly_cloud_cover_high = hourly.Variables(16).ValuesAsNumpy()
hourly_sunshine_duration = hourly.Variables(17).ValuesAsNumpy()
hourly_et0_fao_evapotranspiration = hourly.Variables(18).ValuesAsNumpy()
hourly_potential_evapotranspiration = hourly.Variables(19).ValuesAsNumpy()
hourly_wind_speed_10m = hourly.Variables(20).ValuesAsNumpy()
hourly_wind_speed_100m = hourly.Variables(21).ValuesAsNumpy()
hourly_wind_speed_200m = hourly.Variables(22).ValuesAsNumpy()
hourly_wind_direction_10m = hourly.Variables(23).ValuesAsNumpy()
hourly_wind_direction_100m = hourly.Variables(24).ValuesAsNumpy()
hourly_wind_direction_200m = hourly.Variables(25).ValuesAsNumpy()
hourly_wind_gusts_10m = hourly.Variables(26).ValuesAsNumpy()
hourly_cape = hourly.Variables(27).ValuesAsNumpy()
hourly_convective_inhibition = hourly.Variables(28).ValuesAsNumpy()
hourly_total_column_integrated_water_vapour = hourly.Variables(29).ValuesAsNumpy()
hourly_surface_temperature = hourly.Variables(30).ValuesAsNumpy()
hourly_vapour_pressure_deficit = hourly.Variables(31).ValuesAsNumpy()
hourly_soil_temperature_0_to_7cm = hourly.Variables(32).ValuesAsNumpy()
hourly_soil_temperature_7_to_28cm = hourly.Variables(33).ValuesAsNumpy()
hourly_soil_temperature_28_to_100cm = hourly.Variables(34).ValuesAsNumpy()
hourly_soil_temperature_100_to_255cm = hourly.Variables(35).ValuesAsNumpy()
hourly_soil_moisture_0_to_7cm = hourly.Variables(36).ValuesAsNumpy()
hourly_soil_moisture_7_to_28cm = hourly.Variables(37).ValuesAsNumpy()
hourly_soil_moisture_28_to_100cm = hourly.Variables(38).ValuesAsNumpy()
hourly_soil_moisture_100_to_255cm = hourly.Variables(39).ValuesAsNumpy()
hourly_is_day = hourly.Variables(40).ValuesAsNumpy()
hourly_temperature_2m_min = hourly.Variables(41).ValuesAsNumpy()
hourly_wet_bulb_temperature_2m = hourly.Variables(42).ValuesAsNumpy()
hourly_temperature_2m_max = hourly.Variables(43).ValuesAsNumpy()
hourly_precipitation_type = hourly.Variables(44).ValuesAsNumpy()
hourly_sea_level_height_msl = hourly.Variables(45).ValuesAsNumpy()
hourly_ocean_current_velocity = hourly.Variables(46).ValuesAsNumpy()
hourly_ocean_current_direction = hourly.Variables(47).ValuesAsNumpy()
hourly_lightning_density = hourly.Variables(48).ValuesAsNumpy()
hourly_sea_ice_thickness = hourly.Variables(49).ValuesAsNumpy()
hourly_roughness_length = hourly.Variables(50).ValuesAsNumpy()
hourly_albedo = hourly.Variables(51).ValuesAsNumpy()
hourly_k_index = hourly.Variables(52).ValuesAsNumpy()
hourly_snowfall_water_equivalent = hourly.Variables(53).ValuesAsNumpy()
hourly_snow_depth_water_equivalent = hourly.Variables(54).ValuesAsNumpy()
hourly_shortwave_radiation = hourly.Variables(55).ValuesAsNumpy()
hourly_direct_radiation = hourly.Variables(56).ValuesAsNumpy()
hourly_diffuse_radiation = hourly.Variables(57).ValuesAsNumpy()
hourly_terrestrial_radiation = hourly.Variables(58).ValuesAsNumpy()
hourly_global_tilted_irradiance = hourly.Variables(59).ValuesAsNumpy()
hourly_direct_normal_irradiance = hourly.Variables(60).ValuesAsNumpy()
hourly_shortwave_radiation_instant = hourly.Variables(61).ValuesAsNumpy()
hourly_terrestrial_radiation_instant = hourly.Variables(62).ValuesAsNumpy()
hourly_global_tilted_irradiance_instant = hourly.Variables(63).ValuesAsNumpy()
hourly_direct_normal_irradiance_instant = hourly.Variables(64).ValuesAsNumpy()
hourly_direct_radiation_instant = hourly.Variables(65).ValuesAsNumpy()
hourly_diffuse_radiation_instant = hourly.Variables(66).ValuesAsNumpy()
hourly_temperature_1000hPa = hourly.Variables(67).ValuesAsNumpy()
hourly_temperature_925hPa = hourly.Variables(68).ValuesAsNumpy()
hourly_temperature_850hPa = hourly.Variables(69).ValuesAsNumpy()
hourly_temperature_700hPa = hourly.Variables(70).ValuesAsNumpy()
hourly_temperature_600hPa = hourly.Variables(71).ValuesAsNumpy()
hourly_temperature_500hPa = hourly.Variables(72).ValuesAsNumpy()
hourly_temperature_400hPa = hourly.Variables(73).ValuesAsNumpy()
hourly_temperature_250hPa = hourly.Variables(74).ValuesAsNumpy()
hourly_temperature_300hPa = hourly.Variables(75).ValuesAsNumpy()
hourly_temperature_200hPa = hourly.Variables(76).ValuesAsNumpy()
hourly_temperature_150hPa = hourly.Variables(77).ValuesAsNumpy()
hourly_temperature_100hPa = hourly.Variables(78).ValuesAsNumpy()
hourly_temperature_50hPa = hourly.Variables(79).ValuesAsNumpy()
hourly_relative_humidity_1000hPa = hourly.Variables(80).ValuesAsNumpy()
hourly_relative_humidity_925hPa = hourly.Variables(81).ValuesAsNumpy()
hourly_relative_humidity_850hPa = hourly.Variables(82).ValuesAsNumpy()
hourly_relative_humidity_700hPa = hourly.Variables(83).ValuesAsNumpy()
hourly_relative_humidity_600hPa = hourly.Variables(84).ValuesAsNumpy()
hourly_relative_humidity_500hPa = hourly.Variables(85).ValuesAsNumpy()
hourly_relative_humidity_400hPa = hourly.Variables(86).ValuesAsNumpy()
hourly_relative_humidity_300hPa = hourly.Variables(87).ValuesAsNumpy()
hourly_relative_humidity_200hPa = hourly.Variables(88).ValuesAsNumpy()
hourly_relative_humidity_250hPa = hourly.Variables(89).ValuesAsNumpy()
hourly_relative_humidity_150hPa = hourly.Variables(90).ValuesAsNumpy()
hourly_relative_humidity_100hPa = hourly.Variables(91).ValuesAsNumpy()
hourly_relative_humidity_50hPa = hourly.Variables(92).ValuesAsNumpy()
hourly_cloud_cover_1000hPa = hourly.Variables(93).ValuesAsNumpy()
hourly_cloud_cover_925hPa = hourly.Variables(94).ValuesAsNumpy()
hourly_cloud_cover_700hPa = hourly.Variables(95).ValuesAsNumpy()
hourly_cloud_cover_850hPa = hourly.Variables(96).ValuesAsNumpy()
hourly_cloud_cover_600hPa = hourly.Variables(97).ValuesAsNumpy()
hourly_cloud_cover_500hPa = hourly.Variables(98).ValuesAsNumpy()
hourly_cloud_cover_400hPa = hourly.Variables(99).ValuesAsNumpy()
hourly_cloud_cover_300hPa = hourly.Variables(100).ValuesAsNumpy()
hourly_cloud_cover_250hPa = hourly.Variables(101).ValuesAsNumpy()
hourly_cloud_cover_200hPa = hourly.Variables(102).ValuesAsNumpy()
hourly_cloud_cover_150hPa = hourly.Variables(103).ValuesAsNumpy()
hourly_cloud_cover_100hPa = hourly.Variables(104).ValuesAsNumpy()
hourly_cloud_cover_50hPa = hourly.Variables(105).ValuesAsNumpy()
hourly_wind_speed_1000hPa = hourly.Variables(106).ValuesAsNumpy()
hourly_wind_speed_925hPa = hourly.Variables(107).ValuesAsNumpy()
hourly_wind_speed_850hPa = hourly.Variables(108).ValuesAsNumpy()
hourly_wind_speed_700hPa = hourly.Variables(109).ValuesAsNumpy()
hourly_wind_speed_600hPa = hourly.Variables(110).ValuesAsNumpy()
hourly_wind_speed_500hPa = hourly.Variables(111).ValuesAsNumpy()
hourly_wind_speed_400hPa = hourly.Variables(112).ValuesAsNumpy()
hourly_wind_speed_300hPa = hourly.Variables(113).ValuesAsNumpy()
hourly_wind_speed_200hPa = hourly.Variables(114).ValuesAsNumpy()
hourly_wind_speed_250hPa = hourly.Variables(115).ValuesAsNumpy()
hourly_wind_speed_150hPa = hourly.Variables(116).ValuesAsNumpy()
hourly_wind_speed_100hPa = hourly.Variables(117).ValuesAsNumpy()
hourly_wind_speed_50hPa = hourly.Variables(118).ValuesAsNumpy()
hourly_wind_direction_1000hPa = hourly.Variables(119).ValuesAsNumpy()
hourly_wind_direction_925hPa = hourly.Variables(120).ValuesAsNumpy()
hourly_wind_direction_600hPa = hourly.Variables(121).ValuesAsNumpy()
hourly_wind_direction_850hPa = hourly.Variables(122).ValuesAsNumpy()
hourly_wind_direction_700hPa = hourly.Variables(123).ValuesAsNumpy()
hourly_wind_direction_500hPa = hourly.Variables(124).ValuesAsNumpy()
hourly_wind_direction_400hPa = hourly.Variables(125).ValuesAsNumpy()
hourly_wind_direction_300hPa = hourly.Variables(126).ValuesAsNumpy()
hourly_wind_direction_200hPa = hourly.Variables(127).ValuesAsNumpy()
hourly_wind_direction_250hPa = hourly.Variables(128).ValuesAsNumpy()
hourly_wind_direction_150hPa = hourly.Variables(129).ValuesAsNumpy()
hourly_wind_direction_100hPa = hourly.Variables(130).ValuesAsNumpy()
hourly_wind_direction_50hPa = hourly.Variables(131).ValuesAsNumpy()
hourly_vertical_velocity_1000hPa = hourly.Variables(132).ValuesAsNumpy()
hourly_vertical_velocity_925hPa = hourly.Variables(133).ValuesAsNumpy()
hourly_vertical_velocity_850hPa = hourly.Variables(134).ValuesAsNumpy()
hourly_vertical_velocity_700hPa = hourly.Variables(135).ValuesAsNumpy()
hourly_vertical_velocity_600hPa = hourly.Variables(136).ValuesAsNumpy()
hourly_vertical_velocity_500hPa = hourly.Variables(137).ValuesAsNumpy()
hourly_vertical_velocity_400hPa = hourly.Variables(138).ValuesAsNumpy()
hourly_vertical_velocity_300hPa = hourly.Variables(139).ValuesAsNumpy()
hourly_vertical_velocity_250hPa = hourly.Variables(140).ValuesAsNumpy()
hourly_vertical_velocity_200hPa = hourly.Variables(141).ValuesAsNumpy()
hourly_vertical_velocity_150hPa = hourly.Variables(142).ValuesAsNumpy()
hourly_vertical_velocity_100hPa = hourly.Variables(143).ValuesAsNumpy()
hourly_vertical_velocity_50hPa = hourly.Variables(144).ValuesAsNumpy()
hourly_geopotential_height_1000hPa = hourly.Variables(145).ValuesAsNumpy()
hourly_geopotential_height_925hPa = hourly.Variables(146).ValuesAsNumpy()
hourly_geopotential_height_850hPa = hourly.Variables(147).ValuesAsNumpy()
hourly_geopotential_height_700hPa = hourly.Variables(148).ValuesAsNumpy()
hourly_geopotential_height_600hPa = hourly.Variables(149).ValuesAsNumpy()
hourly_geopotential_height_500hPa = hourly.Variables(150).ValuesAsNumpy()
hourly_geopotential_height_300hPa = hourly.Variables(151).ValuesAsNumpy()
hourly_geopotential_height_400hPa = hourly.Variables(152).ValuesAsNumpy()
hourly_geopotential_height_250hPa = hourly.Variables(153).ValuesAsNumpy()
hourly_geopotential_height_200hPa = hourly.Variables(154).ValuesAsNumpy()
hourly_geopotential_height_150hPa = hourly.Variables(155).ValuesAsNumpy()
hourly_geopotential_height_100hPa = hourly.Variables(156).ValuesAsNumpy()
hourly_geopotential_height_50hPa = hourly.Variables(157).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
hourly_data["dew_point_2m"] = hourly_dew_point_2m
hourly_data["apparent_temperature"] = hourly_apparent_temperature
hourly_data["precipitation"] = hourly_precipitation
hourly_data["rain"] = hourly_rain
hourly_data["showers"] = hourly_showers
hourly_data["snowfall"] = hourly_snowfall
hourly_data["runoff"] = hourly_runoff
hourly_data["visibility"] = hourly_visibility
hourly_data["weather_code"] = hourly_weather_code
hourly_data["surface_pressure"] = hourly_surface_pressure
hourly_data["pressure_msl"] = hourly_pressure_msl
hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
hourly_data["cloud_cover"] = hourly_cloud_cover
hourly_data["cloud_cover_mid"] = hourly_cloud_cover_mid
hourly_data["cloud_cover_high"] = hourly_cloud_cover_high
hourly_data["sunshine_duration"] = hourly_sunshine_duration
hourly_data["et0_fao_evapotranspiration"] = hourly_et0_fao_evapotranspiration
hourly_data["potential_evapotranspiration"] = hourly_potential_evapotranspiration
hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
hourly_data["wind_speed_100m"] = hourly_wind_speed_100m
hourly_data["wind_speed_200m"] = hourly_wind_speed_200m
hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
hourly_data["wind_direction_100m"] = hourly_wind_direction_100m
hourly_data["wind_direction_200m"] = hourly_wind_direction_200m
hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
hourly_data["cape"] = hourly_cape
hourly_data["convective_inhibition"] = hourly_convective_inhibition
hourly_data["total_column_integrated_water_vapour"] = hourly_total_column_integrated_water_vapour
hourly_data["surface_temperature"] = hourly_surface_temperature
hourly_data["vapour_pressure_deficit"] = hourly_vapour_pressure_deficit
hourly_data["soil_temperature_0_to_7cm"] = hourly_soil_temperature_0_to_7cm
hourly_data["soil_temperature_7_to_28cm"] = hourly_soil_temperature_7_to_28cm
hourly_data["soil_temperature_28_to_100cm"] = hourly_soil_temperature_28_to_100cm
hourly_data["soil_temperature_100_to_255cm"] = hourly_soil_temperature_100_to_255cm
hourly_data["soil_moisture_0_to_7cm"] = hourly_soil_moisture_0_to_7cm
hourly_data["soil_moisture_7_to_28cm"] = hourly_soil_moisture_7_to_28cm
hourly_data["soil_moisture_28_to_100cm"] = hourly_soil_moisture_28_to_100cm
hourly_data["soil_moisture_100_to_255cm"] = hourly_soil_moisture_100_to_255cm
hourly_data["is_day"] = hourly_is_day
hourly_data["temperature_2m_min"] = hourly_temperature_2m_min
hourly_data["wet_bulb_temperature_2m"] = hourly_wet_bulb_temperature_2m
hourly_data["temperature_2m_max"] = hourly_temperature_2m_max
hourly_data["precipitation_type"] = hourly_precipitation_type
hourly_data["sea_level_height_msl"] = hourly_sea_level_height_msl
hourly_data["ocean_current_velocity"] = hourly_ocean_current_velocity
hourly_data["ocean_current_direction"] = hourly_ocean_current_direction
hourly_data["lightning_density"] = hourly_lightning_density
hourly_data["sea_ice_thickness"] = hourly_sea_ice_thickness
hourly_data["roughness_length"] = hourly_roughness_length
hourly_data["albedo"] = hourly_albedo
hourly_data["k_index"] = hourly_k_index
hourly_data["snowfall_water_equivalent"] = hourly_snowfall_water_equivalent
hourly_data["snow_depth_water_equivalent"] = hourly_snow_depth_water_equivalent
hourly_data["shortwave_radiation"] = hourly_shortwave_radiation
hourly_data["direct_radiation"] = hourly_direct_radiation
hourly_data["diffuse_radiation"] = hourly_diffuse_radiation
hourly_data["terrestrial_radiation"] = hourly_terrestrial_radiation
hourly_data["global_tilted_irradiance"] = hourly_global_tilted_irradiance
hourly_data["direct_normal_irradiance"] = hourly_direct_normal_irradiance
hourly_data["shortwave_radiation_instant"] = hourly_shortwave_radiation_instant
hourly_data["terrestrial_radiation_instant"] = hourly_terrestrial_radiation_instant
hourly_data["global_tilted_irradiance_instant"] = hourly_global_tilted_irradiance_instant
hourly_data["direct_normal_irradiance_instant"] = hourly_direct_normal_irradiance_instant
hourly_data["direct_radiation_instant"] = hourly_direct_radiation_instant
hourly_data["diffuse_radiation_instant"] = hourly_diffuse_radiation_instant
hourly_data["temperature_1000hPa"] = hourly_temperature_1000hPa
hourly_data["temperature_925hPa"] = hourly_temperature_925hPa
hourly_data["temperature_850hPa"] = hourly_temperature_850hPa
hourly_data["temperature_700hPa"] = hourly_temperature_700hPa
hourly_data["temperature_600hPa"] = hourly_temperature_600hPa
hourly_data["temperature_500hPa"] = hourly_temperature_500hPa
hourly_data["temperature_400hPa"] = hourly_temperature_400hPa
hourly_data["temperature_250hPa"] = hourly_temperature_250hPa
hourly_data["temperature_300hPa"] = hourly_temperature_300hPa
hourly_data["temperature_200hPa"] = hourly_temperature_200hPa
hourly_data["temperature_150hPa"] = hourly_temperature_150hPa
hourly_data["temperature_100hPa"] = hourly_temperature_100hPa
hourly_data["temperature_50hPa"] = hourly_temperature_50hPa
hourly_data["relative_humidity_1000hPa"] = hourly_relative_humidity_1000hPa
hourly_data["relative_humidity_925hPa"] = hourly_relative_humidity_925hPa
hourly_data["relative_humidity_850hPa"] = hourly_relative_humidity_850hPa
hourly_data["relative_humidity_700hPa"] = hourly_relative_humidity_700hPa
hourly_data["relative_humidity_600hPa"] = hourly_relative_humidity_600hPa
hourly_data["relative_humidity_500hPa"] = hourly_relative_humidity_500hPa
hourly_data["relative_humidity_400hPa"] = hourly_relative_humidity_400hPa
hourly_data["relative_humidity_300hPa"] = hourly_relative_humidity_300hPa
hourly_data["relative_humidity_200hPa"] = hourly_relative_humidity_200hPa
hourly_data["relative_humidity_250hPa"] = hourly_relative_humidity_250hPa
hourly_data["relative_humidity_150hPa"] = hourly_relative_humidity_150hPa
hourly_data["relative_humidity_100hPa"] = hourly_relative_humidity_100hPa
hourly_data["relative_humidity_50hPa"] = hourly_relative_humidity_50hPa
hourly_data["cloud_cover_1000hPa"] = hourly_cloud_cover_1000hPa
hourly_data["cloud_cover_925hPa"] = hourly_cloud_cover_925hPa
hourly_data["cloud_cover_700hPa"] = hourly_cloud_cover_700hPa
hourly_data["cloud_cover_850hPa"] = hourly_cloud_cover_850hPa
hourly_data["cloud_cover_600hPa"] = hourly_cloud_cover_600hPa
hourly_data["cloud_cover_500hPa"] = hourly_cloud_cover_500hPa
hourly_data["cloud_cover_400hPa"] = hourly_cloud_cover_400hPa
hourly_data["cloud_cover_300hPa"] = hourly_cloud_cover_300hPa
hourly_data["cloud_cover_250hPa"] = hourly_cloud_cover_250hPa
hourly_data["cloud_cover_200hPa"] = hourly_cloud_cover_200hPa
hourly_data["cloud_cover_150hPa"] = hourly_cloud_cover_150hPa
hourly_data["cloud_cover_100hPa"] = hourly_cloud_cover_100hPa
hourly_data["cloud_cover_50hPa"] = hourly_cloud_cover_50hPa
hourly_data["wind_speed_1000hPa"] = hourly_wind_speed_1000hPa
hourly_data["wind_speed_925hPa"] = hourly_wind_speed_925hPa
hourly_data["wind_speed_850hPa"] = hourly_wind_speed_850hPa
hourly_data["wind_speed_700hPa"] = hourly_wind_speed_700hPa
hourly_data["wind_speed_600hPa"] = hourly_wind_speed_600hPa
hourly_data["wind_speed_500hPa"] = hourly_wind_speed_500hPa
hourly_data["wind_speed_400hPa"] = hourly_wind_speed_400hPa
hourly_data["wind_speed_300hPa"] = hourly_wind_speed_300hPa
hourly_data["wind_speed_200hPa"] = hourly_wind_speed_200hPa
hourly_data["wind_speed_250hPa"] = hourly_wind_speed_250hPa
hourly_data["wind_speed_150hPa"] = hourly_wind_speed_150hPa
hourly_data["wind_speed_100hPa"] = hourly_wind_speed_100hPa
hourly_data["wind_speed_50hPa"] = hourly_wind_speed_50hPa
hourly_data["wind_direction_1000hPa"] = hourly_wind_direction_1000hPa
hourly_data["wind_direction_925hPa"] = hourly_wind_direction_925hPa
hourly_data["wind_direction_600hPa"] = hourly_wind_direction_600hPa
hourly_data["wind_direction_850hPa"] = hourly_wind_direction_850hPa
hourly_data["wind_direction_700hPa"] = hourly_wind_direction_700hPa
hourly_data["wind_direction_500hPa"] = hourly_wind_direction_500hPa
hourly_data["wind_direction_400hPa"] = hourly_wind_direction_400hPa
hourly_data["wind_direction_300hPa"] = hourly_wind_direction_300hPa
hourly_data["wind_direction_200hPa"] = hourly_wind_direction_200hPa
hourly_data["wind_direction_250hPa"] = hourly_wind_direction_250hPa
hourly_data["wind_direction_150hPa"] = hourly_wind_direction_150hPa
hourly_data["wind_direction_100hPa"] = hourly_wind_direction_100hPa
hourly_data["wind_direction_50hPa"] = hourly_wind_direction_50hPa
hourly_data["vertical_velocity_1000hPa"] = hourly_vertical_velocity_1000hPa
hourly_data["vertical_velocity_925hPa"] = hourly_vertical_velocity_925hPa
hourly_data["vertical_velocity_850hPa"] = hourly_vertical_velocity_850hPa
hourly_data["vertical_velocity_700hPa"] = hourly_vertical_velocity_700hPa
hourly_data["vertical_velocity_600hPa"] = hourly_vertical_velocity_600hPa
hourly_data["vertical_velocity_500hPa"] = hourly_vertical_velocity_500hPa
hourly_data["vertical_velocity_400hPa"] = hourly_vertical_velocity_400hPa
hourly_data["vertical_velocity_300hPa"] = hourly_vertical_velocity_300hPa
hourly_data["vertical_velocity_250hPa"] = hourly_vertical_velocity_250hPa
hourly_data["vertical_velocity_200hPa"] = hourly_vertical_velocity_200hPa
hourly_data["vertical_velocity_150hPa"] = hourly_vertical_velocity_150hPa
hourly_data["vertical_velocity_100hPa"] = hourly_vertical_velocity_100hPa
hourly_data["vertical_velocity_50hPa"] = hourly_vertical_velocity_50hPa
hourly_data["geopotential_height_1000hPa"] = hourly_geopotential_height_1000hPa
hourly_data["geopotential_height_925hPa"] = hourly_geopotential_height_925hPa
hourly_data["geopotential_height_850hPa"] = hourly_geopotential_height_850hPa
hourly_data["geopotential_height_700hPa"] = hourly_geopotential_height_700hPa
hourly_data["geopotential_height_600hPa"] = hourly_geopotential_height_600hPa
hourly_data["geopotential_height_500hPa"] = hourly_geopotential_height_500hPa
hourly_data["geopotential_height_300hPa"] = hourly_geopotential_height_300hPa
hourly_data["geopotential_height_400hPa"] = hourly_geopotential_height_400hPa
hourly_data["geopotential_height_250hPa"] = hourly_geopotential_height_250hPa
hourly_data["geopotential_height_200hPa"] = hourly_geopotential_height_200hPa
hourly_data["geopotential_height_150hPa"] = hourly_geopotential_height_150hPa
hourly_data["geopotential_height_100hPa"] = hourly_geopotential_height_100hPa
hourly_data["geopotential_height_50hPa"] = hourly_geopotential_height_50hPa

hourly_dataframe = pd.DataFrame(data = hourly_data)

# Aggregate to 1 value per day by taking the mean of all hourly values
hourly_dataframe['date'] = pd.to_datetime(hourly_dataframe['date']).dt.normalize()
daily_dataframe = hourly_dataframe.groupby('date').mean(numeric_only=True).reset_index()
print(f"\nDaily data ({len(daily_dataframe)} days)\n", daily_dataframe)

# ---------------------------------------------------------
# DATABASE CONNECTIE MET ENV VARIABELEN
# ---------------------------------------------------------

from db_config import get_database_url

# Build a safe, SSL-enabled SQLAlchemy URL (Azure requires SSL)
db_connection_str = get_database_url()
print("Database connection configured (password hidden)")
db_connection = create_engine(db_connection_str)

try:
    TABLE_NAME = os.getenv("TABLE_ECMWF", "ECMWF")
    daily_dataframe.to_sql(TABLE_NAME, db_connection, if_exists='replace', index=False)
    print("Succes! De data is weggeschreven naar de SQL database.")

except Exception as e:
    print(f"Er ging iets mis bij het wegschrijven naar SQL: {e}")

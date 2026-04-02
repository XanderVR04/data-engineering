import os
import sys
import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_config import get_database_url

engine = create_engine(get_database_url())

TABLE_GEO = os.getenv("TABLE_KMI_AWS", "kmi_aws_hourly")
TABLE_UKKEL = os.getenv("TABLE_KAGGLE_UKKEL", "kaggle_station_daily")
TABLE_ECMWF = os.getenv("TABLE_ECMWF", "ECMWF")

# GEO (KMI): average across stations per day, wind speeds in m/s
geo = pd.read_sql(
    f'''
    SELECT date::date                  AS date,
           AVG(wind_speed_10m)         AS GEO_windspeed_10m,
           AVG(wind_speed_avg_30m)     AS GEO_windspeed_30m
    FROM "{TABLE_GEO}"
    GROUP BY date::date
    ''',
    engine,
)

# Ukkel: average across stations per day, wind speeds in m/s
ukkel = pd.read_sql(
    f'''
    SELECT timestamp::date             AS date,
           AVG(wind_speed_10m)         AS Ukkel_windspeed_10m,
           AVG(wind_speed_avg_30m)     AS Ukkel_windspeed_30m
    FROM "{TABLE_UKKEL}"
    GROUP BY timestamp::date
    ''',
    engine,
)

# ECMWF: one row per day, wind speed in m/s (no 30m available)
ecmwf = pd.read_sql(
    f'''
    SELECT date::date                  AS date,
           wind_speed_10m              AS ECMWF_windspeed_10m
    FROM "{TABLE_ECMWF}"
    ''',
    engine,
)

# Merge all three on date using outer joins so no data is lost
combined = geo.merge(ukkel, on='date', how='outer') \
              .merge(ecmwf, on='date', how='outer')

combined['date'] = pd.to_datetime(combined['date'], utc=True).dt.normalize().dt.tz_localize(None)
combined = combined.sort_values('date').reset_index(drop=True)

wind_cols = [c for c in combined.columns if c != 'date']
combined[wind_cols] = combined[wind_cols].round(2)

combined.to_sql('wind', engine, if_exists='replace', index=False)
print(f"Succes! Wind tabel aangemaakt met {len(combined)} rijen.")
print(f"Brondata blijft bewaard in tabellen: {TABLE_GEO}, {TABLE_UKKEL}, {TABLE_ECMWF}")
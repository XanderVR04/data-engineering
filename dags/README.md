# DAGs

This folder contains the single Airflow DAG that drives the wind-data pipeline.

## wind_pipeline

**File:** `wind_pipeline.py`  
**Schedule:** every 10 minutes (`*/10 * * * *`)  
**Max active runs:** 1 (a new run waits if the previous one is still running)

### Task flow

```
fetch_kmi_data ──┐
fetch_ecmwf_data ─┼──► wind
fetch_ukkel_data ─┘
```

| Task | Script | What it does |
|---|---|---|
| `fetch_kmi_data` | `scripts/GEO.py` | Fetches hourly station data from the KMI/AWS WFS API, aggregates to daily averages per station, stores in `GEO` table |
| `fetch_ecmwf_data` | `scripts/ECMWF.py` | Fetches ECMWF IFS forecast from Open-Meteo (~7 day window), aggregates to daily averages, stores in `ECMWF` table |
| `fetch_ukkel_data` | `scripts/kaggleUkkel.py` | Downloads daily weather data from Kaggle (8 Belgian stations), stores in `kaggle_ukkel` table |
| `wind` | `scripts/wind.py` | Joins the three tables into one `wind_combined` table with daily wind speeds (m/s) per source |

The three fetch tasks are independent and run sequentially (SequentialExecutor). The `wind` task runs only after all three succeed.

### Notes

- All scripts run as subprocesses to isolate import-time side effects from the Airflow scheduler.
- Environment variables (`DB_*`, `KAGGLE_USERNAME`, `KAGGLE_KEY`) are loaded from `scripts/.env` via `docker-compose.yml`.
- Scripts are mounted into the container at `/app/scripts` via the `.:/app` volume in `docker-compose.yml`.
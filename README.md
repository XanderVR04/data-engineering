# wind-data

Docker-based data engineering pipeline that collects Belgian wind and weather data from three sources, aggregates it to daily averages, and stores everything in PostgreSQL — orchestrated by Apache Airflow.

## Architecture

```
KMI/AWS WFS API  ──────────────────► GEO table          ─┐
Kaggle (Uccle stations) ───────────► kaggle_ukkel table  ─┼──► wind_combined table
Open-Meteo ECMWF IFS ──────────────► ECMWF table         ─┘

                 Airflow DAG (wind_pipeline) — runs every 10 minutes
```

## Data sources

| Script | Source | Table | Granularity |
|---|---|---|---|
| `GEO.py` | KMI/AWS WFS API — Belgian weather stations | `GEO` | Hourly → daily average |
| `kaggleUkkel.py` | Kaggle — 8 Belgian meteorological stations | `kaggle_ukkel` | Daily |
| `ECMWF.py` | Open-Meteo ECMWF IFS forecast | `ECMWF` | Hourly → daily average |
| `wind.py` | Combines the three sources above | `wind_combined` | Daily |

The `wind_combined` table is the main output. It contains daily wind speed values (m/s) for each source side by side:

| Column | Description |
|---|---|
| `date` | Day |
| `geo_windspeed_10m` | KMI — wind speed at 10 m (m/s) |
| `geo_windspeed_30m` | KMI — wind speed at 30 m (m/s) |
| `ukkel_windspeed_10m` | Kaggle Uccle — wind speed at 10 m (m/s) |
| `ukkel_windspeed_30m` | Kaggle Uccle — wind speed at 30 m (m/s) |
| `ecmwf_windspeed_10m` | ECMWF forecast — wind speed at 10 m (m/s) |

---

## Services & Ports

| Service | URL | Credentials |
|---|---|---|
| PostgreSQL | `localhost:5432` | user: `admin` / pass: `password` / db: `weather_db` |
| pgAdmin | http://localhost:8080 | `admin@admin.com` / `admin` |
| Airflow | http://localhost:8085 | see step 3 below |

---

## Setup

### 1. Configure credentials

Copy the example env file and fill in your Kaggle API key:

```bash
cp scripts/.env.example scripts/.env
```

Edit `scripts/.env`:
```env
DB_HOST=postgres
DB_PORT=5432
DB_NAME=weather_db
DB_USER=admin
DB_PASSWORD=password
DB_SSLMODE=disable
DATABASE_URL=postgresql://admin:password@postgres:5432/weather_db?sslmode=disable

TABLE_KMI_AWS=GEO
TABLE_KAGGLE_UKKEL=kaggle_ukkel
TABLE_ECMWF=ECMWF

KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_api_key
```

> Your Kaggle API key is at https://www.kaggle.com → Account → API → *Create New Token*.

### 2. Build and start

```bash
docker compose up --build -d
```

This starts PostgreSQL, pgAdmin and Airflow. The first build takes a few minutes to install all Python dependencies.

### 3. Get the Airflow password

On first start, Airflow generates an admin password. Retrieve it with:

```bash
docker logs airflow | Select-String "Login with username"   # PowerShell
docker logs airflow | grep "Login with username"            # bash/macOS
```

Then open http://localhost:8085 and log in with `admin` and the generated password.

### 4. Run the pipeline

1. In the Airflow UI, find the **`wind_pipeline`** DAG
2. Toggle it **on** (unpause)
3. Click ▶ **Trigger DAG** to run it immediately

The pipeline fetches all three sources, aggregates to daily averages, and builds the `wind_combined` table. It re-runs automatically every 10 minutes to refresh the ECMWF forecast data.

### 5. View data in pgAdmin

1. Open http://localhost:8080
2. Log in with `admin@admin.com` / `admin`
3. Add a server:
   - **Host:** `postgres`
   - **Port:** `5432`
   - **Database:** `weather_db`
   - **Username:** `admin`
   - **Password:** `password`
4. Browse to **Schemas → public → Tables** to see `GEO`, `ECMWF`, `kaggle_ukkel` and `wind_combined`

---

## Stopping

```bash
docker compose down          # stop containers, keep database
docker compose down -v       # stop containers and delete database volume
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `postgres` | PostgreSQL hostname |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `weather_db` | Database name |
| `DB_USER` | `admin` | Database user |
| `DB_PASSWORD` | `password` | Database password |
| `DB_SSLMODE` | `disable` | SSL mode (`disable` for local, `require` for Azure) |
| `TABLE_KMI_AWS` | `GEO` | Table name for KMI data |
| `TABLE_KAGGLE_UKKEL` | `kaggle_ukkel` | Table name for Kaggle Uccle data |
| `TABLE_ECMWF` | `ECMWF` | Table name for ECMWF data |
| `KAGGLE_USERNAME` | — | Kaggle username |
| `KAGGLE_KEY` | — | Kaggle API key |
| `FORCE_RELOAD` | `0` | Set to `1` to re-fetch KMI data even if the table already has rows |
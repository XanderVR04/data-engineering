# data-engineering

Docker-based data engineering pipeline that collects Belgian wind, energy consumption, energy production, and solar data from multiple sources, stores everything in PostgreSQL, and visualizes it with Grafana — orchestrated by Apache Airflow.

## Architecture

```
                        WIND PIPELINE (every 10 minutes)
KMI/AWS WFS API  ──────────────────► GEO table         ─┐
Kaggle (Uccle stations) ───────────► kaggle_ukkel table ─┼──► wind table
Open-Meteo ECMWF IFS ──────────────► ECMWF table        ─┘

                        MANUAL PIPELINES (trigger once)
consumptie.csv ────────────────────────────────────────────► consumptie table
productie.csv  ────────────────────────────────────────────► productie table
zon.csv        ────────────────────────────────────────────► zon table

                        VISUALIZATION
PostgreSQL ─────────────────────────────────────────────────► Grafana dashboards (4x)
```

## Data sources

### Wind pipeline (automated)

| Script | Source | Table | Granularity |
|---|---|---|---|
| `GEO.py` | KMI/AWS WFS API — Belgian weather stations | `GEO` (intermediate) | Hourly → daily average |
| `kaggleUkkel.py` | Kaggle — 8 Belgian meteorological stations | `kaggle_ukkel` (intermediate) | Daily |
| `ECMWF.py` | Open-Meteo ECMWF IFS forecast | `ECMWF` (intermediate) | Hourly → daily average |
| `wind.py` | Combines the three sources above | `wind` | Daily |

The intermediate tables (`GEO`, `kaggle_ukkel`, `ECMWF`) are dropped after each run. The `wind` table is the final output and contains daily wind speed values (m/s) from all sources side by side:

| Column | Description |
|---|---|
| `date` | Day |
| `GEO_windspeed_10m` | KMI — wind speed at 10 m (m/s) |
| `GEO_windspeed_30m` | KMI — wind speed at 30 m (m/s) |
| `Ukkel_windspeed_10m` | Kaggle Uccle — wind speed at 10 m (m/s) |
| `Ukkel_windspeed_30m` | Kaggle Uccle — wind speed at 30 m (m/s) |
| `ECMWF_windspeed_10m` | ECMWF forecast — wind speed at 10 m (m/s) |

### Static pipelines (manual trigger)

| Script | Source file | Table |
|---|---|---|
| `data-scripts/consumptie.py` | `data/consumptie.csv` | `consumptie` |
| `data-scripts/productie.py` | `data/productie.csv` | `productie` |
| `data-scripts/zon.py` | `data/zon.csv` | `zon` |

---

## Services & Ports

| Service | URL | Credentials |
|---|---|---|
| PostgreSQL | `localhost:5432` | user: `admin` / pass: `password` / db: `weather_db` |
| pgAdmin | http://localhost:8080 | `admin@admin.com` / `admin` |
| Airflow | http://localhost:8085 | see step 3 below |
| Grafana | http://localhost:3000 | `admin` / `admin` (you'll be prompted to change it on first login) |

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

### 2. Add static CSV files

Place the required CSV files in the `data/` directory before running the manual pipelines:

```
data/
├── consumptie.csv
├── productie.csv
└── zon.csv
```

### 3. Build and start

```bash
docker compose up --build -d
```

This starts PostgreSQL, pgAdmin, Airflow, and Grafana. The first build takes a few minutes to install all Python dependencies.

### 4. Get the Airflow password

On first start, Airflow generates an admin password. Retrieve it with:

```bash
docker logs airflow | grep "Login with username"            # bash/macOS
docker logs airflow | Select-String "Login with username"   # PowerShell
```

Then open http://localhost:8085 and log in with `admin` and the generated password.

### 5. Run the pipelines

Open http://localhost:8085 and find the following DAGs:

#### Wind pipeline (automated)

1. Find the **`wind_pipeline`** DAG
2. Toggle it **on** (unpause)
3. Click ▶ **Trigger DAG** to run it immediately

The pipeline fetches all three wind sources, aggregates them to daily averages, builds the `wind` table, and then drops the intermediate tables. It re-runs automatically every 10 minutes to refresh the ECMWF forecast data.

#### Static pipelines (manual, run once)

Trigger each of the following DAGs manually after placing the corresponding CSV files in `data/`:

| DAG | Reads | Writes |
|---|---|---|
| `consumptie_pipeline` | `data/consumptie.csv` | `consumptie` table |
| `productie_pipeline` | `data/productie.csv` | `productie` table |
| `zon_pipeline` | `data/zon.csv` | `zon` table |

### 6. Visualize data in Grafana

1. Open http://localhost:3000
2. Log in with `admin` / `admin` (you'll be asked to set a new password on first login)
3. Go to **Dashboards** in the left sidebar — four dashboards are pre-loaded automatically

| Dashboard | Data source | Description |
|---|---|---|
| **Wind Data** | `wind` table | Wind speeds from all three sources (KMI, Kaggle, ECMWF) |
| **Consumptie** | `consumptie` table | Energy consumption over time |
| **Productie** | `productie` table | Energy production over time |
| **Zon** | `zon` table | Solar data over time |

The Wind Data dashboard contains four panels:

| Panel | What you see |
|---|---|
| **Wind Speeds — All Sources** | All 5 wind speed series on one chart for easy comparison |
| **GEO (KMI) Wind Speeds** | KMI station data at 10 m and 30 m height |
| **Ukkel Wind Speeds** | Kaggle Uccle station data at 10 m and 30 m height |
| **ECMWF Wind Speed** | ECMWF forecast wind speed at 10 m |

Where a source has no data for a date (NULL), the line is intentionally broken — no value is invented to bridge the gap. All dashboards auto-refresh every 5 minutes.

### 7. View data in pgAdmin

1. Open http://localhost:8080
2. Log in with `admin@admin.com` / `admin`
3. Add a server:
   - **Host:** `postgres`
   - **Port:** `5432`
   - **Database:** `weather_db`
   - **Username:** `admin`
   - **Password:** `password`
4. Browse to **Schemas → public → Tables** to see all tables (`wind`, `consumptie`, `productie`, `zon`)

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
| `MAX_WAIT_FOR_DB_SECONDS` | `120` | How long to wait for PostgreSQL to become available on startup |
| `DB_RETRY_SLEEP_SECONDS` | `5` | Seconds between database connection retries |

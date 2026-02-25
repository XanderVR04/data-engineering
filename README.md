# Weather Data Engineering Project (Beginner-Friendly Guide)

## 1) What is this repository?

This repository is a **small Data Engineering project** that shows a complete mini pipeline:

1. **Download weather data** from Kaggle
2. **Clean and transform** it with Python/Pandas
3. **Store it in PostgreSQL** (a relational database)
4. **Expose it through an API** using FastAPI
5. Optionally inspect the database in **pgAdmin**

In short: it turns raw CSV files into queryable API data.

---

## 2) Who is this for?

This README is written for someone who:
- has little/no experience with Data Engineering,
- has basic programming knowledge,
- and wants to understand both **how to run** and **how it works internally**.

If you can run commands in a terminal and read basic Python, you’re good.

---

## 3) Big-picture architecture

The project runs with Docker Compose and starts **4 containers/services**:

- **db** → PostgreSQL database
- **data_import** → Python ETL script (Extract, Transform, Load)
- **api** → FastAPI app to read data from database
- **pgadmin** → optional web UI to inspect the database

### Data flow in one sentence

`Kaggle CSV -> /data/raw -> Pandas DataFrame -> PostgreSQL (weather_data table) -> FastAPI /data endpoint`

---

## 4) Project structure (what each file/folder does)

```text
.
├── docker-compose.yml
├── init.sql
├── README.md
├── api/
│   ├── api.py
│   ├── Dockerfile
│   └── requirements.txt
├── data_import/
│   ├── data_import.py
│   └── Dockerfile
└── data/
    ├── raw/
    └── postgres/
```

### Root files

- **docker-compose.yml**
  - Central orchestrator.
  - Defines all services, networking, ports, and environment variables.
  - Makes all containers start together.

- **init.sql**
  - Very small SQL script:
  - `DROP TABLE IF EXISTS weather_data;`
  - Used as a reset/cleanup script if needed.

### `api/` folder

- **api.py**
  - Creates FastAPI app.
  - Connects to PostgreSQL.
  - Waits until `weather_data` exists and has records before serving data.
  - Exposes endpoint: `GET /data?limit=100`.

- **Dockerfile**
  - Builds Python image for API service.
  - Installs dependencies from `requirements.txt`.
  - Starts server with uvicorn.

- **requirements.txt**
  - Python packages needed by API (`fastapi`, `uvicorn`, `psycopg2-binary`, ...).

### `data_import/` folder

- **data_import.py**
  - The ETL script in 3 steps:
    1. download data from Kaggle
    2. clean/process with Pandas
    3. write final table to PostgreSQL

- **Dockerfile**
  - Builds Python image for ETL service.
  - Installs Pandas, SQLAlchemy, psycopg2, Kaggle API.
  - Runs ETL script once.

### `data/` folder

- **raw/**
  - Local storage for raw CSV files.

- **postgres/**
  - PostgreSQL persistent data directory.
  - Keeps DB data even if containers are recreated.

---

## 5) How the pipeline works (step by step)

When you run Docker Compose, this is the practical sequence:

1. **PostgreSQL starts** (`db` service).
2. **ETL service starts** (`data_import` service).
3. ETL checks Kaggle credentials and downloads dataset to `/data/raw`.
4. ETL reads CSV into a Pandas DataFrame.
5. ETL transforms data:
   - parses timestamps,
   - adds `year` and `month`,
   - removes unnecessary columns,
   - maps station codes to station names.
6. ETL writes final DataFrame into Postgres table `weather_data`.
7. **API service starts** (`api` service).
8. API keeps retrying database connection until data exists.
9. API serves `/data` endpoint.
10. You can query weather rows via browser/Postman/curl.

---

## 6) Prerequisites

You need:

1. **Docker Desktop** installed and running
2. Internet access (for Kaggle download, unless CSV already exists in `data/raw`)
3. (Optional) A Kaggle account + API credentials

---

## 7) Before first run: important credential note

Credentials are loaded from a root `.env` file.

1. Copy `.env.example` to `.env`.
2. Fill in your own values (especially `KAGGLE_USERNAME`, `KAGGLE_KEY`, and passwords).
3. Keep `.env` private (it is ignored by git).

---

## 8) How to run the project

Open terminal in the repository root and run:

```bash
docker compose up --build
```

What this does:
- builds images for `api` and `data_import`,
- starts all services,
- shows logs in your terminal.

### First run can take a while

- Pulling Docker images and installing packages may take several minutes.
- ETL will wait for DB readiness if needed.

---

## 9) How to test if everything works

### A) API health check (simple)

Open browser:

- `http://localhost:8000/data`

You should see JSON output (a list of weather rows).

### B) Limit rows

- `http://localhost:8000/data?limit=10`

Returns only 10 records.

### C) Using curl (PowerShell)

```powershell
curl "http://localhost:8000/data?limit=5"
```

---

## 10) Using pgAdmin (optional but useful)

pgAdmin is available at:

- `http://localhost:8080`

Login credentials (from compose):
- Email: `admin@admin.com`
- Password: `root`

Then connect to PostgreSQL server with:
- Host: `db`
- Port: `5432`
- Database: `weatherdb`
- User: `myuser`
- Password: `mypassword`

This lets you inspect `weather_data` table directly.

---

## 11) Detailed explanation of the Python code

## `data_import/data_import.py` (ETL)

### Step 1: Download (`step_1_download_raw_data`)
- Checks if `KAGGLE_USERNAME` and `KAGGLE_KEY` exist.
- If yes: authenticates Kaggle API and downloads dataset zip, then unzips.
- If no: fallback mode tries to use an existing CSV file in `data/raw`.

### Step 2: Transform (`step_2_process_data_to_dataframe`)
- Reads CSV with `pandas.read_csv`.
- Converts `timestamp` to datetime.
- Adds derived columns: `year`, `month`.
- Removes noisy columns (`FID`, `qc_flags`) if present.
- Maps station numeric `code` to human-readable `station_name`.

### Step 3: Load (`step_3_write_to_sql`)
- Waits until database is reachable.
- Uses SQLAlchemy engine to write DataFrame to SQL.
- `if_exists='replace'` means table is recreated on each ETL run.

### Main block
- Creates raw data folder if missing.
- Runs all ETL steps in order.
- Stops with clear messages if CSV cannot be found.

---

## `api/api.py` (Data API)

### Database connection
- `get_db_connection()` retries connection in a loop.
- Useful because container startup timing is not deterministic.

### Data availability gate
- `check_data_availability()` loops until:
  - `weather_data` table exists, and
  - row count is greater than 0.
- This prevents serving empty responses during startup.

### Endpoint
- `GET /data?limit=100`
- Executes SQL:

```sql
SELECT * FROM weather_data LIMIT %s
```

- Returns rows as JSON dictionaries.

---

## 12) Common issues + fixes

### 1) API keeps waiting forever

Possible causes:
- ETL failed
- table not created
- database not reachable

Check logs:

```bash
docker compose logs -f data_import
docker compose logs -f api
docker compose logs -f db
```

### 2) Kaggle download fails

- Credentials missing or invalid
- Rate limits/network issue

Fix:
- verify `KAGGLE_USERNAME` and `KAGGLE_KEY`
- or manually place CSV in `data/raw` and rerun

### 3) Port already in use

If `5432`, `8000`, or `8080` is taken, stop conflicting app or change port mapping in compose.

### 4) Dirty database state

To fully reset:

```bash
docker compose down -v
```

Then start fresh:

```bash
docker compose up --build
```

---

## 13) What to improve next (if you continue this project)

Good next steps for learning and better engineering practice:

1. Add more filtered endpoints (by station/date/month).
2. Add data validation (null handling, schema checks).
3. Replace `if_exists='replace'` with incremental loading.
4. Add tests for ETL transformations and API endpoints.
5. Add structured logging instead of plain `print` statements.

---

## 14) Quick command cheat sheet

Start all services:

```bash
docker compose up --build
```

Start in background:

```bash
docker compose up -d --build
```

View running containers:

```bash
docker compose ps
```

View logs:

```bash
docker compose logs -f
```

Stop services:

```bash
docker compose down
```

Stop and remove volumes (full reset):

```bash
docker compose down -v
```

---

## 15) Final summary

This repo demonstrates a complete beginner-level Data Engineering lifecycle:

- **Extract** from an external source (Kaggle)
- **Transform** with Pandas
- **Load** into Postgres
- **Serve** with FastAPI

It is a great foundation for learning how raw data becomes a usable API in a containerized environment.

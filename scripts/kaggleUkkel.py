import psycopg2
from psycopg2.extras import RealDictCursor
import time
import pandas as pd
from sqlalchemy import create_engine
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import sys
from typing import Optional

# Ensure db_config (in the same directory) is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_config import get_database_url, get_db_settings

# FastAPI is only needed when running as an API server (loader container).
# When running as an Airflow task we skip it to avoid dependency conflicts.
try:
    from fastapi import FastAPI, HTTPException
    app = FastAPI()
    _has_fastapi = True
except ImportError:
    _has_fastapi = False

RAW_DATA_PATH = "/tmp/kaggle_raw"
DATASET_NAME = "chrlkadm/belgium-weather-data-from-two-stations-since-2020"
CSV_FILE_NAME = "aws_1day.csv"

DATABASE_URL = get_database_url()
TABLE_NAME = os.getenv("TABLE_KAGGLE_UKKEL", "kaggle_station_daily")

# Safety valves: prevent infinite waiting in containers.
MAX_WAIT_FOR_DB_SECONDS = int(os.getenv("MAX_WAIT_FOR_DB_SECONDS", "120"))
MAX_WAIT_FOR_TABLE_SECONDS = int(os.getenv("MAX_WAIT_FOR_TABLE_SECONDS", "300"))
DB_RETRY_SLEEP_SECONDS = float(os.getenv("DB_RETRY_SLEEP_SECONDS", "2"))
TABLE_CHECK_SLEEP_SECONDS = float(os.getenv("TABLE_CHECK_SLEEP_SECONDS", "5"))

STATION_MAPPING = {
    6455: "Dourbes",
    6472: "Humain",
    6484: "Buzenol",
    6438: "Stabroek",
    6464: "Retie",
    6477: "Uccle",
    6459: "Ernage",
    6439: "Sint-Katelijne-Waver",
}


def _mask(s: Optional[str], keep_last: int = 2) -> str:
    if not s:
        return "<empty>"
    if len(s) <= keep_last:
        return "*" * len(s)
    return "*" * (len(s) - keep_last) + s[-keep_last:]


def _debug_db_settings(prefix: str = "DB"):
    s = get_db_settings()
    print(
        f"{prefix}: host={s.host} port={s.port} db={s.name} user={s.user} sslmode={getattr(s, 'sslmode', None)} password={_mask(s.password)}"
    )


def get_db_connection(max_wait_seconds: int = MAX_WAIT_FOR_DB_SECONDS):
    start = time.time()

    while True:
        try:
            s = get_db_settings()
            conn = psycopg2.connect(
                host=s.host,
                database=s.name,
                user=s.user,
                password=s.password,
                port=str(s.port),
                sslmode=getattr(s, "sslmode", "require"),
                cursor_factory=RealDictCursor,
            )
            return conn
        except psycopg2.OperationalError as e:
            if time.time() - start > max_wait_seconds:
                _debug_db_settings(prefix="DB (final)")
                raise RuntimeError(
                    f"Database not ready after {max_wait_seconds}s (host={get_db_settings().host}). Last error: {e}"
                )
            print(f"Database nog niet klaar, sleeping {DB_RETRY_SLEEP_SECONDS}s...")
            time.sleep(DB_RETRY_SLEEP_SECONDS)


def wait_for_db_engine(max_wait_seconds: int = MAX_WAIT_FOR_DB_SECONDS):
    start = time.time()
    last_err = None

    while True:
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect():
                return engine
        except Exception as e:
            last_err = e
            if time.time() - start > max_wait_seconds:
                raise RuntimeError(f"SQLAlchemy engine not ready after {max_wait_seconds}s. Last error: {e}")
            print(f"   Database nog niet klaar... {DB_RETRY_SLEEP_SECONDS} seconden wachten.")
            time.sleep(DB_RETRY_SLEEP_SECONDS)


def step_1_download_raw_data():
    print(f"STAP 1: Start download data van Kaggle ({DATASET_NAME})...")

    # Allow running without Kaggle creds if the CSV is already present (e.g., mounted volume).
    if not (os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY")):
        print(
            "GEEN Kaggle credentials gevonden! Zorg dat KAGGLE_USERNAME en KAGGLE_KEY in docker-compose staan."
        )
        if os.path.exists(os.path.join(RAW_DATA_PATH, CSV_FILE_NAME)):
            print("   Gebruik bestaand bestand in raw folder.")
            return True
        # Try any CSV in the folder as fallback.
        if os.path.isdir(RAW_DATA_PATH):
            found_files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".csv")]
            if found_files:
                print(f"   Gebruik bestaand CSV bestand in raw folder: {found_files[0]}")
                return True
        return False

    try:
        kaggle_api = KaggleApi()
        kaggle_api.authenticate()
        kaggle_api.dataset_download_files(DATASET_NAME, path=RAW_DATA_PATH, unzip=True)
        print(f"Download voltooid. Data staat in {RAW_DATA_PATH}")
        return True
    except Exception as e:
        print(f"Fout tijdens downloaden: {e}")
        return False


def step_2_process_data_to_dataframe(csv_path):
    print("STAP 2: Databron omzetten naar Pandas DataFrame & Schonen...")

    df = pd.read_csv(csv_path)
    print(f"   Origineel: {len(df)} rijen, {len(df.columns)} kolommen.")

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["year"] = df["timestamp"].dt.year
        df["month"] = df["timestamp"].dt.month
        print("   Datums verwerkt en extra tijd-kolommen toegevoegd.")

    columns_to_drop = ["FID", "qc_flags"]

    existing_cols_to_drop = [c for c in columns_to_drop if c in df.columns]
    if existing_cols_to_drop:
        df = df.drop(columns=existing_cols_to_drop)
        print(f"   Overbodige kolommen verwijderd: {existing_cols_to_drop}")

    if "code" in df.columns:
        df["station_name"] = df["code"].map(STATION_MAPPING).fillna("Unknown")
        print("   Station namen toegevoegd op basis van code.")

    print(f"   Status na cleaning: {len(df.columns)} kolommen over.")
    return df


def step_3_write_to_sql(df, table_name=TABLE_NAME):
    print(f"STAP 3: DataFrame wegschrijven naar SQL database (tabel: {table_name})...")

    engine = wait_for_db_engine()

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f" Succes! Tabel '{table_name}' aangemaakt en gevuld met SQL.")


def check_data_availability(max_wait_seconds: int = MAX_WAIT_FOR_TABLE_SECONDS):
    conn = get_db_connection()
    cur = conn.cursor()

    start = time.time()
    while True:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            count = cur.fetchone()["count"]
            if count > 0:
                print(f"Data gevonden! ({count} records). API start nu op.")
                break
            else:
                print("Tabel bestaat, maar is leeg. Wachten op data import...")
        except psycopg2.errors.UndefinedTable:
            print(f"Tabel '{TABLE_NAME}' nog niet gevonden. Wachten op data import...")
            conn.rollback()
        except Exception as e:
            print(f"Database check fout: {e}")
            conn.rollback()

        if time.time() - start > max_wait_seconds:
            cur.close()
            conn.close()
            raise RuntimeError(
                f"Timeout: table '{TABLE_NAME}' not available after {max_wait_seconds}s. "
                "Ingestion likely failed (missing Kaggle credentials or CSV)."
            )

        time.sleep(TABLE_CHECK_SLEEP_SECONDS)

    cur.close()
    conn.close()


def run_data_import_pipeline():
    print(" Data Import Service Start")

    os.makedirs(RAW_DATA_PATH, exist_ok=True)

    success = step_1_download_raw_data()
    if not success:
        raise RuntimeError(
            f"Kaggle download failed and no cached CSV found in {RAW_DATA_PATH}. "
            "Set KAGGLE_USERNAME/KAGGLE_KEY or mount a CSV into /data/raw."
        )

    csv_path = os.path.join(RAW_DATA_PATH, CSV_FILE_NAME)

    if not os.path.exists(csv_path):
        found_files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith(".csv")]
        if found_files:
            csv_path = os.path.join(RAW_DATA_PATH, found_files[0])
            print(f"   CSV_FILE_NAME niet gevonden, gebruik: {os.path.basename(csv_path)}")
        else:
            raise RuntimeError("Geen CSV bestand gevonden na download. Stop.")

    df_weather = step_2_process_data_to_dataframe(csv_path)
    step_3_write_to_sql(df_weather, TABLE_NAME)

    print(" Klaar! Script sluit af.")


if __name__ == "__main__":
    run_data_import_pipeline()

if _has_fastapi:
    @app.get("/")
    def read_root():
        return {"message": "Welcome to the Weather Data API"}

    @app.get("/data")
    def get_weather_data(limit: int = 100):
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT * FROM {TABLE_NAME} LIMIT %s", (limit,))
            rows = cur.fetchall()
            return rows
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        finally:
            cur.close()
            conn.close()
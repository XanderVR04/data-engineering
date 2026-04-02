import pandas as pd
import requests
import io
from sqlalchemy import create_engine, text
import time
import os
import sys

# Ensure db_config (in the same directory) is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_config import get_database_url

DATABASE_URL = get_database_url()
RMI_URL = "http://opendata.meteo.be/geoserver/aws/wfs"
TABLE_NAME = os.getenv("TABLE_KMI_AWS", "kmi_aws_hourly")


def _table_has_rows(engine, table_name: str, schema: str = "public") -> bool:
    """Return True if the table exists (in given schema) and contains at least 1 row."""
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name = :table
                    )
                    """
                ),
                {"schema": schema, "table": table_name},
            ).scalar()

            if not exists:
                print("Table doesnt exist.")
                return False

            # Quote schema/table to avoid issues with case / reserved words.
            row = conn.execute(text(f'SELECT 1 FROM "{schema}"."{table_name}" LIMIT 1')).first()
            return row is not None
    except Exception:
        # If the check fails for any reason, fall back to loading.
        return False


def run_pipeline():
    try:
        engine = create_engine(DATABASE_URL)

        force_reload = os.getenv("FORCE_RELOAD", "0") == "1"
        if not force_reload and _table_has_rows(engine, TABLE_NAME, schema=os.getenv("DB_SCHEMA", "public")):
            print(
                f"Tabel '{TABLE_NAME}' bevat al data. Overslaan (zet FORCE_RELOAD=1 om te forceren)."
            )
            return

        params = {
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": "aws:aws_1hour",
            "outputFormat": "csv"
        }

        print(f"Data ophalen van KMI...")
        response = requests.get(RMI_URL, params=params, timeout=45)
        response.raise_for_status()

        # Gebruik latin-1 of utf-8 afhankelijk van de RMI output
        df = pd.read_csv(io.BytesIO(response.content), encoding='ISO-8859-1')

        if df.empty:
            print("Geen data ontvangen van de API.")
            return

        # Kolomnamen opschonen: 'aws:temp' -> 'temp'
        df.columns = [c.split(':')[-1].replace('"', '').strip().lower() for c in df.columns]

        # Aggregate to 1 value per day per station by taking the mean
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df['date'] = df['timestamp'].dt.normalize()
        drop_cols = [c for c in ['fid', 'the_geom', 'qc_flags', 'timestamp'] if c in df.columns]
        df = df.drop(columns=drop_cols)
        df = df.groupby(['date', 'code']).mean(numeric_only=True).reset_index()

        print(f"Opslaan in database (tabel: {TABLE_NAME})...")
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)

        print(f"Succes! {len(df)} rijen verwerkt.")

    except Exception as e:
        print(f"Fout: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
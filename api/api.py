from fastapi import FastAPI, HTTPException
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import time

app = FastAPI()

def get_db_connection():
    # Wacht tot de database beschikbaar is
    while True:
        try:
            conn = psycopg2.connect(
                host="db",
                database=os.getenv("POSTGRES_DB", "weatherdb"),
                user=os.getenv("POSTGRES_USER", "myuser"),
                password=os.getenv("POSTGRES_PASSWORD", "mypassword"),
                port="5432",
                cursor_factory=RealDictCursor
            )
            return conn
        except psycopg2.OperationalError:
            print("Database nog niet klaar, sleeping 2s...")
            time.sleep(2)

def check_data_availability():
    """Controleert of de 'weather_data' tabel bestaat en data bevat."""
    conn = get_db_connection()
    cur = conn.cursor()
    while True:
        try:
            cur.execute("SELECT COUNT(*) FROM weather_data")
            count = cur.fetchone()['count']
            if count > 0:
                print(f"✅ Data gevonden! ({count} records). API start nu op.")
                break
            else:
                print("⏳ Tabel bestaat, maar is leeg. Wachten op data import...")
        except psycopg2.errors.UndefinedTable:
             print("⏳ Tabel 'weather_data' nog niet gevonden. Wachten op data import...")
             conn.rollback() # Reset transactie na fout
        except Exception as e:
            print(f"⚠️  Database check fout: {e}")
        
        time.sleep(5)
    
    cur.close()
    conn.close()

# Start de check bij het opstarten van de API
print("🚀 API container gestart. Wachten op data...")
check_data_availability()

app = FastAPI()
def read_root():
    return {"message": "Welcome to the Weather Data API"}

@app.get("/data")
def get_weather_data(limit: int = 100):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM weather_data LIMIT %s", (limit,))
        rows = cur.fetchall()
        return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

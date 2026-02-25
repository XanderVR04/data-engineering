import os
import time
import pandas as pd
from sqlalchemy import create_engine
from kaggle.api.kaggle_api_extended import KaggleApi

# Mappen
RAW_DATA_PATH = "/data/raw"
DATASET_NAME = "chrlkadm/belgium-weather-data-from-two-stations-since-2020"
CSV_FILE_NAME = "aws_1day.csv" 

# Database
DB_HOST = "db"
DB_NAME = os.getenv("POSTGRES_DB", "weatherdb")
DB_USER = os.getenv("POSTGRES_USER", "myuser")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "mypassword")
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

def step_1_download_raw_data():
    print(f"STAP 1: Start download data van Kaggle ({DATASET_NAME})...")
    
    # Check credentials
    if not (os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY")):
        print("GEEN Kaggle credentials gevonden! Zorg dat KAGGLE_USERNAME en KAGGLE_KEY in docker-compose staan.")
        # Fallback voor demo als download faalt: kijken of het bestand er al staat (handmatig)
        if os.path.exists(os.path.join(RAW_DATA_PATH, CSV_FILE_NAME)):
            print("   Gebruik bestaand bestand in raw folder.")
            return True
        return False

    try:
        api = KaggleApi()
        api.authenticate()
        # Download en unzip direct in de RAW folder
        api.dataset_download_files(DATASET_NAME, path=RAW_DATA_PATH, unzip=True)
        print(f"Download voltooid. Data staat in {RAW_DATA_PATH}")
        return True
    except Exception as e:
        print(f"Fout tijdens downloaden: {e}")
        return False

def step_2_process_data_to_dataframe(csv_path):
    print("STAP 2: Databron omzetten naar Pandas DataFrame & Schonen...")
    
    # Inlezen van de ruwe data
    df = pd.read_csv(csv_path)
    print(f"   Origineel: {len(df)} rijen, {len(df.columns)} kolommen.")
    
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        # Voeg extra kolommen toe voor makkelijke analyse
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        print("   Datums verwerkt en extra tijd-kolommen toegevoegd.")

    columns_to_drop = ['FID', 'qc_flags'] # FID is rommel id, qc_flags is technische json
    
    # Als we geen landbouw doen, kunnen we bodem sensoren ook droppen (optioneel, ik laat ze nu nog even staan)
    # columns_to_drop.extend(['temp_soil_avg_5cm', 'temp_soil_avg_10cm', 'temp_soil_avg_20cm', 'temp_soil_avg_50cm'])

    existing_cols_to_drop = [c for c in columns_to_drop if c in df.columns]
    if existing_cols_to_drop:
        df = df.drop(columns=existing_cols_to_drop)
        print(f"   Overbodige kolommen verwijderd: {existing_cols_to_drop}")

    # We hebben 'code', maar geen naam. Laten we die mappen.
    # Op basis van kennis over dataset (6472 = Humain, 6447 = Ukkel)
    station_mapping = {
        6455: 'Dourbes',
        6472: 'Humain',
        6484: 'Buzenol', 
        6438: 'Diepenbeek',
        6438: 'Stabroek',
        6464: 'Retie',
        6447: 'Uccle',
        6459: 'Ernage',
        6439: 'Sint-Katelijne-Waver'
    }
    if 'code' in df.columns:
        df['station_name'] = df['code'].map(station_mapping).fillna('Unknown')
        print("   Station namen toegevoegd op basis van code.")

    print(f"   Status na cleaning: {len(df.columns)} kolommen over.")
    return df

def step_3_write_to_sql(df, table_name="weather_data"):
    print(f"STAP 3: DataFrame wegschrijven naar SQL database (tabel: {table_name})...")
    
    # Wacht op database verbinding
    engine = None
    while engine is None:
        try:
            engine = create_engine(DB_URL)
            with engine.connect() as conn:
                pass
        except Exception:
            print("   Database nog niet klaar... 2 seconden wachten.")
            time.sleep(2)

    try:
        # Hier gebeurt de magie: Pandas regelt de CREATE TABLE en INSERT statements
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f" Succes! Tabel '{table_name}' aangemaakt en gevuld met SQL.")
    except Exception as e:
        print(f" Fout bij database import: {e}")

if __name__ == "__main__":
    print(" Data Import Service Start")
    
    # Zorg dat map bestaat
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    
    # 1. Haal data op (Raw storage)
    success = step_1_download_raw_data()
    
    if success:
        # Bepaal het bestandspad
        csv_path = os.path.join(RAW_DATA_PATH, CSV_FILE_NAME)
        
        # Fallback logica als bestand een andere naam heeft
        if not os.path.exists(csv_path):
            found_files = [f for f in os.listdir(RAW_DATA_PATH) if f.endswith('.csv')]
            if found_files:
                csv_path = os.path.join(RAW_DATA_PATH, found_files[0])
            else:
                print(" Geen CSV bestand gevonden. Stop.")
                exit(1)

        # 2. Omzetten naar DataFrame (Opdracht eis)
        df_weather = step_2_process_data_to_dataframe(csv_path)
        
        # 3. Wegschrijven naar SQL (Opdracht eis)
        step_3_write_to_sql(df_weather, "weather_data")
    
    print(" Klaar! Script sluit af.")

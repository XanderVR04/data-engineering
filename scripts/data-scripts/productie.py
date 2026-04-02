import os
import sys
import pandas as pd
from sqlalchemy import create_engine

# Allow importing db_config from the parent scripts/ directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_config import get_database_url

CSV_PATH = '/app/data/productie.csv'
TABLE_NAME = 'productie'

engine = create_engine(get_database_url())

df = pd.read_csv(CSV_PATH)

df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
print(f"Succes! Tabel '{TABLE_NAME}' aangemaakt met {len(df)} rijen.")

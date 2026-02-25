FROM python:3.9-slim

WORKDIR /app

# Installeeer libraries voor Kaggle, Postgres en Data manipulatie
RUN pip install pandas sqlalchemy psycopg2-binary kaggle

# Kopieer ons script
COPY data_import.py .

# Start het script
CMD ["python", "data_import.py"]

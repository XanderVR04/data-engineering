FROM python:3.9-slim

WORKDIR /app

RUN pip install pandas sqlalchemy psycopg2-binary kaggle

COPY data_import.py .

CMD ["python", "data_import.py"]
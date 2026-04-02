FROM apache/airflow:2.7.1-python3.9

# Switch to root to install system packages and Python deps
USER root

# Install system dependencies required by some Python packages (psycopg2, etc.)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libpq-dev build-essential \
    && rm -rf /var/lib/apt/lists/*

# Restore to airflow user before installing packages so they land in the
# right site-packages (/home/airflow/.local) that the airflow user can see.
USER airflow

# Copy requirements for the ingestion scripts and install them
COPY scripts/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --user -r /tmp/requirements.txt
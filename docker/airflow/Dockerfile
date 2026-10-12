# Official Apache Airflow image (Airflow 3.1.8 on Python 3.11)
FROM apache/airflow:3.1.8-python3.11

# Installing system packages needs the root (admin) user
USER root

# Install Java, which Spark needs to run inside this container.
# default-jre-headless picks the right Java version for this Linux release.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the normal "airflow" user for everything else (safer)
USER airflow

# Copy in the pipeline's library lists
COPY requirements.txt requirements-pipeline.txt /tmp/

# Create a SEPARATE virtual environment for the pipeline libraries
# (PySpark, dbt, boto3...). Keeping them apart from Airflow's own
# libraries avoids version clashes between dbt and Airflow.
# Also create the folder Spark uses to cache the S3 connector.
RUN python -m venv /home/airflow/pipeline-venv \
    && /home/airflow/pipeline-venv/bin/pip install --no-cache-dir -r /tmp/requirements-pipeline.txt \
    && mkdir -p /home/airflow/.ivy2
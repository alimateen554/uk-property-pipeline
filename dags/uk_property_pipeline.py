"""
Airflow DAG for the UK property pipeline.

Runs the four pipeline steps in order, each only after the previous one
succeeds:

    ingest_to_s3 -> clean_with_spark -> load_to_postgres -> dbt_build

Each task simply runs one of the project's existing scripts with the
pipeline virtual environment built into the Airflow image.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

# Where the project folder is mounted inside the Airflow container
# (see the "volumes" section of docker-compose.yml)
PROJECT_DIR = "/opt/airflow/project"

# The separate virtual environment holding PySpark, dbt, boto3 etc.
# (created in docker/airflow/Dockerfile)
PIPELINE_BIN = "/home/airflow/pipeline-venv/bin"

# Jinja template: when a task runs, Airflow replaces this with the
# "years" value chosen for that run (see params below)
YEARS = "{{ params.years }}"

with DAG(
    dag_id="uk_property_pipeline",
    description="Ingest, clean, load and model HM Land Registry Price Paid data",
    # HM Land Registry publishes new data monthly, so run once a month
    schedule="@monthly",
    #Date the pipeline went live. Manual runs must be dated on or after this,
    # and the first automatic monthly run will be 1 November 2026.
    start_date=datetime(2026, 10, 10),
    # Do not go back and run for past months that were missed
    catchup=False,
    # Never let two runs overlap (they would write to the same tables)
    max_active_runs=1,
    # Run-time settings that can be changed when triggering manually
    params={
        "years": Param(
            "2016 2017 2018 2019 2020 2021 2022 2023 2024 2025",
            type="string",
            description="Space-separated years to ingest and load",
        ),
    },
    # Settings applied to every task in this DAG
    default_args={
        # If a task fails (e.g. a network blip), try again twice...
        "retries": 2,
        # ...waiting 5 minutes between attempts
        "retry_delay": timedelta(minutes=5),
    },
    # Labels used to filter DAGs in the Airflow UI
    tags=["uk-property"],
) as dag:
    # Task 1: download the yearly CSVs from HM Land Registry into S3 (bronze)
    ingest = BashOperator(
        task_id="ingest_to_s3",
        bash_command=f"{PIPELINE_BIN}/python src/ingestion/download_to_s3.py {YEARS}",
        cwd=PROJECT_DIR,
    )

    # Task 2: clean the raw CSVs with PySpark and write Parquet to S3 (silver)
    transform = BashOperator(
        task_id="clean_with_spark",
        bash_command=f"{PIPELINE_BIN}/python src/transform/clean_price_paid.py",
        cwd=PROJECT_DIR,
    )

    # Task 3: load the clean Parquet into the Postgres staging table
    load = BashOperator(
        task_id="load_to_postgres",
        bash_command=f"{PIPELINE_BIN}/python src/load/load_to_postgres.py {YEARS}",
        cwd=PROJECT_DIR,
    )

    # Task 4: build the star schema and run all data tests with dbt (gold)
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{PIPELINE_BIN}/dbt build",
        cwd=PROJECT_DIR,
    )

    # Set the order: each arrow means "must finish successfully before"
    ingest >> transform >> load >> dbt_build
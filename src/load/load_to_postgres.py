"""
Load the clean Price Paid data from S3 into the PostgreSQL warehouse.

This is the "L" (Load) step of the ELT pipeline. It takes the cleaned
Parquet files that the PySpark job wrote to the S3 "clean" (silver) layer
and loads them into a staging table in PostgreSQL, ready for dbt to model.

For each requested year, the loader:
1. Reads that year's Parquet files from S3
2. Records a new batch in staging.load_batches with status "running"
3. In ONE transaction: deletes any existing rows for that year, bulk
   loads the new rows with PostgreSQL COPY, and marks the batch "success"
4. If anything fails, the transaction rolls back and the batch is
   marked "failed", so the table is never left half-loaded

Usage (from the project root, with the virtual environment active):
    python src/load/load_to_postgres.py 2024 2025
"""

# --- Standard library imports (built into Python) ---
import io  # in-memory "files", used to build the CSV for COPY
import os  # reading settings from environment variables
import sys  # reading the years typed on the command line
from datetime import datetime, timezone  # timestamps for the audit table

# --- Third-party imports (installed from requirements.txt) ---
import pyarrow.dataset as ds  # reads folders of Parquet files as one dataset
from dotenv import load_dotenv  # loads the settings in .env
from pyarrow import fs  # filesystem layer that lets pyarrow read from S3
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    delete,
    insert,
    text,
    update,
)
from sqlalchemy.engine import URL  # builds database connection strings safely

# MetaData is SQLAlchemy's catalogue of table definitions. Setting
# schema="staging" means every table below is created in the "staging"
# schema (a namespace inside the database), keeping raw loaded data
# separate from the modelled tables dbt will build later.
metadata = MetaData(schema="staging")

# staging.price_paid: one row per property sale.
# The columns mirror the clean Parquet output from the PySpark job,
# plus a batch_id recording which load inserted each row (data lineage).
price_paid = Table(
    "price_paid",
    metadata,
    # Primary key: the database itself rejects duplicate transactions
    Column("transaction_id", String, primary_key=True),
    # BigInteger (64-bit) safely holds even very large sale prices
    Column("price", BigInteger, nullable=False),
    Column("date_of_transfer", Date, nullable=False),
    Column("postcode", String),
    Column("postcode_district", String),
    # String(1) = exactly one character, e.g. "D" for Detached
    Column("property_type", String(1)),
    Column("property_type_desc", String),
    Column("old_new", String(1)),
    Column("is_new_build", Boolean),
    Column("duration", String(1)),
    Column("tenure", String),
    Column("paon", String),
    Column("saon", String),
    Column("street", String),
    Column("locality", String),
    Column("town_city", String),
    Column("district", String),
    Column("county", String),
    Column("ppd_category_type", String(1)),
    Column("record_status", String(1)),
    # index=True speeds up "WHERE year = ..." (used when reloading a year)
    Column("year", Integer, nullable=False, index=True),
    Column("batch_id", Integer, nullable=False),
)

# staging.load_batches: audit table with one row per load attempt.
# It answers "what was loaded, when, how many rows, and did it succeed?",
# and every row in price_paid can be traced back to its batch here.
load_batches = Table(
    "load_batches",
    metadata,
    # autoincrement=True: Postgres hands out 1, 2, 3... automatically
    Column("batch_id", Integer, primary_key=True, autoincrement=True),
    Column("year", Integer, nullable=False),
    # "running" -> "success" or "failed"
    Column("status", String, nullable=False),
    Column("row_count", BigInteger),
    # timezone=True stores timestamps unambiguously (UTC under the hood)
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
)

# List of column names in table order, e.g. ["transaction_id", "price", ...].
# Used to put the DataFrame columns in exactly the order COPY expects.
COPY_COLUMNS = [column.name for column in price_paid.columns]


def get_engine():
    """Create a SQLAlchemy engine using the settings in .env."""
    # URL.create builds the connection string for us and safely escapes
    # any special characters in the password.
    # "postgresql+psycopg" = database type + the driver used to talk to it.
    url = URL.create(
        "postgresql+psycopg",
        username=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST"),
        # Environment variables are always text, so convert the port to a number
        port=int(os.getenv("POSTGRES_PORT")),
        database=os.getenv("POSTGRES_DB"),
    )
    # The engine manages a pool of connections to the database
    return create_engine(url)


def create_tables(engine):
    """Create the staging schema and tables if they do not exist yet."""
    # engine.begin() opens a connection and a transaction, and commits
    # automatically when the "with" block ends without an error
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
    # create_all only creates tables that are missing, so running the
    # loader again never wipes or recreates existing tables
    metadata.create_all(engine)


def read_year(clean_uri, year):
    """Read one year of clean Parquet data into a pandas DataFrame."""
    # from_uri looks at the start of the path and picks the right filesystem:
    # "s3://..." gives an S3 filesystem, a normal folder path gives a local one.
    # This makes the function easy to test without touching AWS.
    filesystem, path = fs.FileSystem.from_uri(clean_uri)

    # Treat the whole folder of Parquet files as one dataset.
    # partitioning="hive" reads the year=2025 folder names as a "year" column.
    dataset = ds.dataset(path, filesystem=filesystem, format="parquet", partitioning="hive")

    # The filter is applied to the folder names, so only that year's
    # files are downloaded (partition pruning), then converted to pandas
    return dataset.to_table(filter=ds.field("year") == year).to_pandas()


def copy_dataframe(conn, df):
    """Bulk load a DataFrame with PostgreSQL COPY (much faster than INSERTs)."""
    # Write the DataFrame to an in-memory CSV "file" (nothing touches disk).
    # No index and no header row: COPY expects just the data, in column order.
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    # Rewind to the start of the buffer so it can be read from the beginning
    buffer.seek(0)

    # SQLAlchemy does not expose COPY directly, so reach through to the
    # underlying psycopg connection. It is the SAME connection, so the COPY
    # still runs inside the transaction started in load_year().
    cursor = conn.connection.driver_connection.cursor()

    # Name the target columns explicitly so the CSV order always matches
    columns = ", ".join(df.columns)
    sql = f"COPY staging.price_paid ({columns}) FROM STDIN WITH (FORMAT csv)"

    # FROM STDIN means "the data will be streamed from this program"
    with cursor.copy(sql) as copy:
        # Send the data in 8 MB chunks to keep memory use steady.
        # The loop stops when read() returns an empty string (end of buffer).
        while chunk := buffer.read(8_000_000):
            copy.write(chunk)


def load_year(engine, clean_uri, year):
    """Load one year as a single tracked, all-or-nothing batch."""
    # Read the data first, so an S3 problem fails before touching the database
    df = read_year(clean_uri, year)

    # Step 1: register the batch as "running".
    # This is its own transaction and is committed straight away, so even
    # if the load below fails, a record of the attempt is kept.
    with engine.begin() as conn:
        batch_id = conn.execute(
            insert(load_batches)
            .values(year=year, status="running", started_at=datetime.now(timezone.utc))
            # RETURNING sends back the batch_id Postgres just generated
            .returning(load_batches.c.batch_id)
        ).scalar_one()

    try:
        # Step 2: replace the year's data in ONE transaction.
        # Either all three statements succeed together, or none of them do.
        with engine.begin() as conn:
            # Remove any rows from a previous load of this year, so reruns
            # replace data instead of duplicating it (idempotent loading)
            conn.execute(delete(price_paid).where(price_paid.c.year == year))

            # Stamp every row with this batch's ID (data lineage)
            df["batch_id"] = batch_id

            # Bulk load the rows, with columns in table order
            copy_dataframe(conn, df[COPY_COLUMNS])

            # Mark the batch as successful, with its row count
            conn.execute(
                update(load_batches)
                .where(load_batches.c.batch_id == batch_id)
                .values(status="success", row_count=len(df), finished_at=datetime.now(timezone.utc))
            )
    except Exception:
        # Step 3: something went wrong. Leaving the "with" block with an
        # error has already rolled back the delete and the COPY, so the
        # year's previous data is untouched. Record the failure in a new
        # transaction, then re-raise so the error message is still shown.
        with engine.begin() as conn:
            conn.execute(
                update(load_batches)
                .where(load_batches.c.batch_id == batch_id)
                .values(status="failed", finished_at=datetime.now(timezone.utc))
            )
        raise

    print(f"{year}: loaded {len(df)} rows (batch {batch_id})")


def main():
    # Load AWS and Postgres settings from .env into environment variables
    load_dotenv()
    bucket = os.getenv("S3_BUCKET")

    # Where the clean Parquet data lives. Defaults to our S3 bucket, but can
    # be overridden with a CLEAN_URI environment variable for local testing.
    # Note: pyarrow uses "s3://" (Spark uses "s3a://" for the same place).
    clean_uri = os.getenv("CLEAN_URI", f"s3://{bucket}/clean/price_paid")

    engine = get_engine()
    create_tables(engine)

    # Years to load are typed after the script name, e.g. 2024 2025.
    # sys.argv[0] is the script name itself, so the years start at [1:].
    years = [int(y) for y in sys.argv[1:]]
    for year in years:
        load_year(engine, clean_uri, year)


# Only run main() when this file is executed directly,
# not when it is imported by another file (e.g. a test)
if __name__ == "__main__":
    main()
"""
Clean the raw HM Land Registry Price Paid data.

Reads the raw yearly CSV files from the S3 "raw" (bronze) layer, applies
type casting, standardisation and data quality rules, then writes the
result to the S3 "clean" (silver) layer as Parquet, partitioned by year.
"""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# The raw files have NO header row, so we supply the 16 column names
# ourselves, in the exact order published by HM Land Registry.
COLUMNS = [
    "transaction_id",  # unique ID for each sale, wrapped in {curly braces}
    "price",  # sale price in pounds
    "date_of_transfer",  # completion date, e.g. "2025-01-20 00:00"
    "postcode",
    "property_type",  # D, S, T, F or O
    "old_new",  # Y = new build, N = existing property
    "duration",  # F = freehold, L = leasehold
    "paon",  # primary address: house number or name
    "saon",  # secondary address: flat or unit number
    "street",
    "locality",
    "town_city",
    "district",
    "county",
    "ppd_category_type",  # A = standard sale, B = additional (e.g. repossession)
    "record_status",  # A = added, C = changed, D = deleted
]

# Read every column as plain text first. Letting Spark guess the types could
# silently turn bad values into NULLs, so we cast each column ourselves in
# clean() where the rules are explicit and visible (schema enforcement).
SCHEMA = StructType([StructField(name, StringType()) for name in COLUMNS])


def build_spark():
    """Start a local Spark session that can read and write S3."""
    return (
        SparkSession.builder.appName("clean_price_paid")
        # Give the Spark driver 3 GB of memory for ~10 million rows
        .config("spark.driver.memory", "3g")
        # Download the hadoop-aws connector, which adds support for s3a:// paths.
        # Version 3.3.4 matches the Hadoop version bundled with PySpark 3.5.
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
        # Point the connector at the London region, where our bucket lives.
        # AWS keys are picked up automatically from environment variables.
        .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com")
        .getOrCreate()
    )


def clean(df):
    """Apply type casting, standardisation and data quality rules."""

    # Translate the single-letter property type codes into readable labels.
    # F.when() works like a SQL CASE WHEN ... THEN ... ELSE ... END.
    property_type_desc = (
        F.when(F.col("property_type") == "D", "Detached")
        .when(F.col("property_type") == "S", "Semi-Detached")
        .when(F.col("property_type") == "T", "Terraced")
        .when(F.col("property_type") == "F", "Flat/Maisonette")
        .otherwise("Other")
    )

    # Translate the tenure code into a readable label
    tenure = (
        F.when(F.col("duration") == "F", "Freehold")
        .when(F.col("duration") == "L", "Leasehold")
        .otherwise("Unknown")
    )

    # Standardise postcodes: remove stray spaces and make them upper case,
    # so "n9 7xy " and "N9 7XY" are treated as the same postcode
    postcode = F.upper(F.trim(F.col("postcode")))

    return (
        # Remove the { and } characters around each transaction ID
        df.withColumn("transaction_id", F.regexp_replace("transaction_id", "[{}]", ""))
        # Convert price from text to a whole number (long = 64-bit integer)
        .withColumn("price", F.col("price").cast("long"))
        # Keep only the date part ("2025-01-20") and convert it to a real date
        .withColumn("date_of_transfer", F.to_date(F.substring("date_of_transfer", 1, 10)))
        # Blank postcodes become NULL instead of an empty string
        .withColumn("postcode", F.when(postcode != "", postcode))
        # Outward code (the part before the space), e.g. "EN1" from "EN1 3AB".
        # Useful for grouping sales by area later on.
        .withColumn("postcode_district", F.split("postcode", " ").getItem(0))
        # Add the readable labels defined above
        .withColumn("property_type_desc", property_type_desc)
        .withColumn("is_new_build", F.col("old_new") == "Y")
        .withColumn("tenure", tenure)
        # Sale year, used to partition the output files
        .withColumn("year", F.year("date_of_transfer"))
        # --- Data quality rules ---
        # 1. A sale must have a positive price
        .filter(F.col("price") > 0)
        # 2. A sale must have a valid date
        .filter(F.col("date_of_transfer").isNotNull())
        # 3. Each transaction should appear only once
        .dropDuplicates(["transaction_id"])
    )


def main():
    # Load settings from .env when running locally. Inside Docker the
    # settings are passed in by docker-compose, so this does nothing.
    load_dotenv()
    bucket = os.getenv("S3_BUCKET")

    # Input and output locations. They default to our S3 bucket but can be
    # overridden with environment variables (useful for local testing).
    raw_path = os.getenv("RAW_PATH", f"s3a://{bucket}/raw/price_paid/")
    clean_path = os.getenv("CLEAN_PATH", f"s3a://{bucket}/clean/price_paid/")

    spark = build_spark()

    # Read every yearly CSV under the raw folder in one go, using our schema
    raw = spark.read.csv(raw_path, schema=SCHEMA)
    cleaned = clean(raw)

    # Write the cleaned data as Parquet, one sub-folder per year.
    # mode("overwrite") replaces previous output, so reruns are safe (idempotent).
    # partitionBy("year") lets later queries read only the years they need.
    cleaned.write.mode("overwrite").partitionBy("year").parquet(clean_path)

    # Read the output back and count the rows to confirm the write worked
    row_count = spark.read.parquet(clean_path).count()
    print(f"Wrote {row_count} clean rows to {clean_path}")

    spark.stop()


# Only run main() when this file is executed directly,
# not when it is imported by another file (e.g. a test)
if __name__ == "__main__":
    main()
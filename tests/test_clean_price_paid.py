"""Unit tests for the Spark cleaning step (src/transform/clean_price_paid.py)."""

import shutil
from datetime import date

import pytest

# Spark needs Java. Skip every test in this file when Java is missing
# (e.g. on a laptop without Java), instead of failing with a confusing
# error. These tests run for real in GitHub Actions, where Java exists.
pytestmark = pytest.mark.skipif(
    shutil.which("java") is None,
    reason="Spark tests need Java (they run in CI and in the Airflow container)",
)


# A fixture is reusable setup. scope="module" means ONE Spark session is
# started for all tests in this file (starting Spark is slow), and the
# code after "yield" shuts it down when the tests finish.
@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()


def make_row(transaction_id="{ABC-1}", price="250000", postcode="EN1 3AB", property_type="T"):
    """Build one raw row in the same 16-column, all-text shape as the real files.

    Defaults describe a normal valid sale; each test overrides only the
    values it cares about.
    """
    return (
        transaction_id, price, "2025-03-15 00:00", postcode, property_type, "N", "F",
        "12", "", "HIGH STREET", "", "LONDON", "ENFIELD", "GREATER LONDON", "A", "A",
    )


def run_clean(spark, rows):
    """Turn raw rows into a Spark DataFrame, clean it, and return the results."""
    # Imported here (not at the top) so the file can still be collected
    # and skipped cleanly on machines without Java
    from src.transform.clean_price_paid import SCHEMA, clean

    return clean(spark.createDataFrame(rows, SCHEMA)).collect()


def test_types_and_derived_columns(spark):
    row = run_clean(spark, [make_row()])[0]
    # Curly braces stripped from the ID
    assert row["transaction_id"] == "ABC-1"
    # Text converted to proper types
    assert row["price"] == 250000
    assert row["date_of_transfer"] == date(2025, 3, 15)
    # Derived columns
    assert row["year"] == 2025
    assert row["postcode_district"] == "EN1"
    assert row["property_type_desc"] == "Terraced"
    assert row["tenure"] == "Freehold"
    assert row["is_new_build"] is False


def test_postcodes_are_standardised(spark):
    rows = run_clean(spark, [make_row(postcode=" n9 7xy "), make_row("{ABC-2}", postcode="")])
    postcodes = {r["transaction_id"]: r["postcode"] for r in rows}
    # Spaces trimmed and converted to upper case
    assert postcodes["ABC-1"] == "N9 7XY"
    # Blank postcode becomes NULL (None in Python)
    assert postcodes["ABC-2"] is None


def test_zero_prices_are_removed(spark):
    rows = run_clean(spark, [make_row(), make_row("{ABC-2}", price="0")])
    # Only the valid sale survives the price > 0 rule
    assert [r["transaction_id"] for r in rows] == ["ABC-1"]


def test_duplicates_are_removed(spark):
    # The same transaction twice should come out once
    rows = run_clean(spark, [make_row(), make_row()])
    assert len(rows) == 1
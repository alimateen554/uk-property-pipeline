"""Unit tests for the load step (src/load/load_to_postgres.py)."""

import pandas as pd

from src.load.load_to_postgres import COPY_COLUMNS, read_year


def test_copy_columns_start_with_id_and_end_with_batch_id():
    # COPY relies on the column order matching the table definition
    assert COPY_COLUMNS[0] == "transaction_id"
    assert COPY_COLUMNS[-1] == "batch_id"


def test_read_year_only_returns_the_requested_year(tmp_path):
    # tmp_path is a built-in pytest fixture: a fresh, empty temporary
    # folder for this test, deleted automatically afterwards.
    # Build a tiny partitioned dataset: year=2024 (2 rows), year=2025 (1 row)
    for year, ids in [(2024, ["a", "b"]), (2025, ["c"])]:
        folder = tmp_path / f"year={year}"
        folder.mkdir()
        pd.DataFrame({"transaction_id": ids}).to_parquet(folder / "part-0.parquet")

    df = read_year(str(tmp_path), 2025)

    # Only the 2025 row should come back (partition pruning works),
    # and the year column should be read from the folder name
    assert list(df["transaction_id"]) == ["c"]
    assert set(df["year"]) == {2025}
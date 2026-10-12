"""Unit tests for the ingestion step (src/ingestion/download_to_s3.py)."""

# MagicMock creates a fake object that accepts any method call,
# so we can stand in for the real S3 client without touching AWS
from unittest.mock import MagicMock

from botocore.exceptions import ClientError

from src.ingestion.download_to_s3 import already_uploaded, s3_key_for_year


def test_s3_key_uses_hive_partition_layout():
    # The S3 path must use the year=YYYY folder style so Spark,
    # Athena and pyarrow can recognise it as a partition
    assert s3_key_for_year(2025) == "raw/price_paid/year=2025/pp-2025.csv"


def test_already_uploaded_is_true_when_object_exists():
    # A fake S3 client whose head_object() succeeds (the file exists)
    s3 = MagicMock()
    assert already_uploaded(s3, "my-bucket", "some/key.csv") is True
    # Check the function asked S3 about the right bucket and key
    s3.head_object.assert_called_once_with(Bucket="my-bucket", Key="some/key.csv")


def test_already_uploaded_is_false_when_object_missing():
    # A fake S3 client whose head_object() raises the same error
    # boto3 raises for a missing file (HTTP 404)
    s3 = MagicMock()
    s3.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    assert already_uploaded(s3, "my-bucket", "some/key.csv") is False
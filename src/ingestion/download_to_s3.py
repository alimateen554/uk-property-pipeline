import os
import sys

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

BASE_URL = "https://price-paid-data.publicdata.landregistry.gov.uk"

# Builds the S3 path for a year, e.g. raw/price_paid/year=2025/pp-2025.csv
def s3_key_for_year(year):
    return f"raw/price_paid/year={year}/pp-{year}.csv"

# Asks S3 whether that file exists. head_object checks without downloading.
def already_uploaded(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False

# Skips the year if it’s already there; otherwise streams the download straight into S3
def ingest_year(s3, bucket, year):
    key = s3_key_for_year(year)
    if already_uploaded(s3, bucket, key):
        print(f"{year}: already in S3, skipping")
        return

    url = f"{BASE_URL}/pp-{year}.csv"
    print(f"{year}: downloading {url}")
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        response.raw.decode_content = True
        s3.upload_fileobj(response.raw, bucket, key)
    print(f"{year}: uploaded to s3://{bucket}/{key}")

# 	Loads settings, connects to S3, and processes each year you type after the script name
def main():
    load_dotenv()
    bucket = os.getenv("S3_BUCKET")
    s3 = boto3.client("s3")
    years = [int(y) for y in sys.argv[1:]]
    for year in years:
        ingest_year(s3, bucket, year)


if __name__ == "__main__":
    main()
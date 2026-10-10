import os

import boto3
from dotenv import load_dotenv

load_dotenv()

bucket = os.getenv("S3_BUCKET")
s3 = boto3.client("s3")

s3.put_object(
    Bucket=bucket,
    Key="test/hello.txt",
    Body=b"Hello from the UK property pipeline",
)
print(f"Uploaded test/hello.txt to {bucket}")

response = s3.list_objects_v2(Bucket=bucket)
for item in response.get("Contents", []):
    print(item["Key"], item["Size"], "bytes")
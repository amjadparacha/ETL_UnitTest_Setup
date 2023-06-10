import os
from datetime import datetime

import boto3
import pytest
from botocore.exceptions import ClientError


ENDPOINT_URL = 'http://localhost:4566'


@pytest.fixture
def init_bucket():
    s3_client = boto3.client("s3",
                             aws_access_key_id='mock',
                             aws_secret_access_key='mock',
                             region_name='us-east-1',
                             endpoint_url=ENDPOINT_URL)

    try:
        response = s3_client.create_bucket(Bucket="raw")
        response = s3_client.create_bucket(Bucket="bronze")
    except ClientError as ce:
        print("Could not create S3 bucket.")
        raise ce

    filename = os.path.join(os.path.dirname(__file__), '7e0d748b-17f3-389b-a8aa-3990bf2bb4e2-2023-01-06T07_20_51.csv')
    try:
        timestamp = datetime.now()
        partition_str = f"year={timestamp.strftime('%Y')}/month={timestamp.strftime('%m')}/day={timestamp.strftime('%d')}"
        response = s3_client.\
            upload_file(filename,
                        "raw", f"input/{partition_str}/7e0d748b-17f3-389b-a8aa-3990bf2bb4e2-2023-01-06T07_20_51.csv")
    except ClientError as ce:
        print("Could not upload file to S3 bucket.")
        raise ce

    print("file upload to s3")

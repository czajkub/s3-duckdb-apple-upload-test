import os

import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv


load_dotenv()

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "apple-health-healthion")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
DEFAULT_EXPIRATION = 300  # 5 minutes

# Initialize S3 client
try:
    s3_client = boto3.client(
        "s3",
        region_name=AWS_REGION,
        # Credentials should be set via environment variables or IAM roles
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
except NoCredentialsError:
    raise Exception("AWS credentials not configured")

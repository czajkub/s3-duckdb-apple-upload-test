import asyncio

from celery import Celery
from fastapi import BackgroundTasks
import boto3

from app.client import s3_client
from app.ducky.duckdb_importer import ParquetImporter


# Celery setup
celery_app = Celery(
    'file_processor',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379',
    include=['app.processor', 'app.main']
)

importer = ParquetImporter()

@celery_app.task
def process_uploaded_file(bucket_name: str, object_key: str):
    """
    Process file uploaded to S3
    """
    # Download file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body']

    # Process the file (example: get file info)

    counter = 1
    chunk_size = 1024*1024
    chunk = file_content.read(chunk_size)
    while chunk:
        print(f"chunk {counter}")
        print(chunk[:10])
        counter += 1
        chunk = file_content.read(chunk_size)

    # Your processing logic here
    # - Image processing
    # - Text extraction
    # - Data analysis
    # - etc.
    print("RECeived file!!!!11")




    # Optionally save results back to S3 or database
    result = {
        "bucket": bucket_name,
        "key": object_key,
        "status": "processed"
    }

    return result


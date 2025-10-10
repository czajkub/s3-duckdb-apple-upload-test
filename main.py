from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
import requests
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import uuid
from datetime import datetime
from typing import Optional
import os
from enum import Enum

app = FastAPI(title="File Upload API", version="1.0.0")

# Configuration
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "apple-health-healthion")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
DEFAULT_EXPIRATION = 300  # 5 minutes

# Initialize S3 client
try:
    s3_client = boto3.client(
        's3',
        region_name=AWS_REGION,
        # Credentials should be set via environment variables or IAM roles
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
except NoCredentialsError:
    raise Exception("AWS credentials not configured")


class FileType(str, Enum):
    JSON = "application/json"
    XML = "application/xml"
    CSV = "text/csv"
    TEXT = "text/plain"


class DataCategory(str, Enum):
    HEALTH_DATA = "health-data"
    WORKOUT_DATA = "workout-data"
    NUTRITION_DATA = "nutrition-data"
    SLEEP_DATA = "sleep-data"


class PresignedURLRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=100, description="Unique user identifier")
    data_category: DataCategory = Field(..., description="Category of data being uploaded")
    file_type: FileType = Field(default=FileType.JSON, description="MIME type of the file")
    filename: Optional[str] = Field(None, max_length=200, description="Optional custom filename")
    expiration_seconds: Optional[int] = Field(
        default=DEFAULT_EXPIRATION,
        ge=60,
        le=3600,
        description="URL expiration time in seconds (1 min - 1 hour)"
    )
    max_file_size: Optional[int] = Field(
        default=50 * 1024 * 1024,  # 50MB
        ge=1024,  # 1KB minimum
        le=500 * 1024 * 1024,  # 500MB maximum
        description="Maximum file size in bytes"
    )


class PresignedURLResponse(BaseModel):
    upload_url: str
    form_fields: dict[str, str]
    file_key: str
    expires_in: int
    max_file_size: int
    content_type: str
    bucket: str


def generate_file_key(user_id: str, data_category: str, filename: Optional[str] = None) -> str:
    """Generate a unique file key for S3 storage"""
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]

    if filename:
        # Clean filename and preserve extension
        clean_filename = "".join(c for c in filename if c.isalnum() or c in ".-_")
        file_key = f"{data_category}/{user_id}/{timestamp}_{unique_id}_{clean_filename}"
    else:
        file_key = f"{data_category}/{user_id}/{timestamp}_{unique_id}.json"

    return file_key


def validate_bucket_exists() -> bool:
    """Check if the S3 bucket exists and is accessible"""
    try:
        s3_client.head_bucket(Bucket=AWS_BUCKET_NAME)
        return True
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            raise HTTPException(status_code=500, detail="S3 bucket not found")
        elif error_code == '403':
            raise HTTPException(status_code=500, detail="Access denied to S3 bucket")
        else:
            raise HTTPException(status_code=500, detail=f"S3 bucket error: {error_code}")


@app.post("/upload/presigned-url", response_model=PresignedURLResponse)
async def create_presigned_upload_url(request: PresignedURLRequest):
    """
    Generate a presigned URL for direct file upload to S3

    This endpoint creates a secure, time-limited URL that allows clients to upload
    files directly to S3 without going through this server.
    """

    # Validate bucket accessibility
    validate_bucket_exists()

    # Generate unique file key
    file_key = generate_file_key(
        user_id=request.user_id,
        data_category=request.data_category.value,
        filename=request.filename
    )

    try:
        # Create presigned URL with conditions
        conditions = [
            ["content-length-range", 1, request.max_file_size],
            {"Content-Type": request.file_type.value}
        ]

        presigned_post = s3_client.generate_presigned_post(
            Bucket=AWS_BUCKET_NAME,
            Key=file_key,
            Fields={
                "Content-Type": request.file_type.value
            },
            Conditions=conditions,
            ExpiresIn=request.expiration_seconds
        )

        return PresignedURLResponse(
            upload_url=presigned_post['url'],
            form_fields=presigned_post['fields'],
            file_key=file_key,
            expires_in=request.expiration_seconds,
            max_file_size=request.max_file_size,
            content_type=request.file_type.value,
            bucket=AWS_BUCKET_NAME
        )

    except ClientError as e:
        error_code = e.response['Error']['Code']
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate presigned URL: {error_code}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error: {str(e)}"
        )


@app.post("/upload/confirm")
async def confirm_upload(file_key: str, user_id: str):
    """
    Confirm that a file was successfully uploaded to S3

    This endpoint verifies the file exists and can trigger post-upload processing
    """
    try:
        # Verify file exists in S3
        response = s3_client.head_object(Bucket=AWS_BUCKET_NAME, Key=file_key)

        file_size = response.get('ContentLength', 0)
        last_modified = response.get('LastModified')
        content_type = response.get('ContentType')

        # Here you could trigger additional processing:
        # - Add record to database
        # - Queue background job for data processing
        # - Send notification
        # - Update user's upload history

        return {
            "status": "success",
            "message": "File upload confirmed",
            "file_info": {
                "key": file_key,
                "size": file_size,
                "content_type": content_type,
                "last_modified": last_modified.isoformat() if last_modified else None
            }
        }

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            raise HTTPException(status_code=404, detail="File not found in S3")
        else:
            raise HTTPException(status_code=500, detail=f"S3 error: {error_code}")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test S3 connectivity
        s3_client.list_objects_v2(Bucket=AWS_BUCKET_NAME, MaxKeys=1)
        return {"status": "healthy", "s3_connection": "ok"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


if __name__ == "__main__":
    import uvicorn
    import asyncio

    # uvicorn.run(app, host="0.0.0.0", port=8000)

    presigned_url = asyncio.run(create_presigned_upload_url(request=PresignedURLRequest(
        filename="text.json",
        user_id="733796381340",
        data_category=DataCategory("health-data")
    )))

    with open('text.json', 'rb') as f:
        contents = f.read()
        response = requests.post(
            presigned_url.upload_url,
            data=presigned_url.form_fields,
            files={'file': contents}
        )


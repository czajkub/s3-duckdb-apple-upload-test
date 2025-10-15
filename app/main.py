from datetime import datetime, UTC
import json
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Request

from app.client import s3_client, AWS_BUCKET_NAME, AWS_REGION
from app.schema import PresignedURLRequest, PresignedURLResponse, SQSMessage, S3Event
from app.processor import celery_app, process_uploaded_file


QUEUE_URL: str = "https://sqs.eu-north-1.amazonaws.com/733796381340/xml_upload"

def generate_file_key(
    user_id: str, filename: Optional[str] = None
) -> str:
    """Generate a unique file key for S3 storage"""
    timestamp = datetime.now(UTC)

    if filename:
        # Clean filename and preserve extension
        clean_filename = "".join(c for c in filename if c.isalnum() or c in ".-_")
        file_key = f"{user_id}/raw/{clean_filename}"
    else:
        file_key = f"{user_id}/raw/{timestamp}.xml"

    return file_key


def validate_bucket_exists() -> bool:
    """Check if the S3 bucket exists and is accessible"""
    try:
        s3_client.head_bucket(Bucket=AWS_BUCKET_NAME)
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            raise HTTPException(status_code=500, detail="S3 bucket not found")
        elif error_code == "403":
            raise HTTPException(status_code=500, detail="Access denied to S3 bucket")
        else:
            raise HTTPException(
                status_code=500, detail=f"S3 bucket error: {error_code}"
            )

async def poll_sqs_messages():
    """
    Poll SQS for messages (alternative to webhook)
    """
    try:
        # Receive messages from SQS
        response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,  # Long polling
            MessageAttributeNames=['All']
        )

        waiter = sqs.get_waiter("stefan")
        res = await waiter.wait(QueueUrl=QUEUE_URL)

        messages = response.get('Messages', [])
        processed_count = 0

        for message in messages:
            try:
                # Parse message body
                message_body = json.loads(message['Body'])

                # Handle S3 notification
                if 'Records' in message_body:
                    for record in message_body['Records']:
                        if record.get('eventSource') == 'aws:s3':
                            bucket_name = record['s3']['bucket']['name']
                            object_key = record['s3']['object']['key']

                            # Enqueue Celery task
                            task = process_uploaded_file.delay(bucket_name, object_key)
                            print(task)
                            processed_count += 1

                # Delete message from queue after processing
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )

            except Exception as e:
                raise

        return {
            "messages_processed": processed_count,
            "total_messages": len(messages),
            "messages": messages
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


app = FastAPI()
sqs = boto3.client("sqs")


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
        filename=request.filename,
    )

    try:
        # Create presigned URL with conditions
        conditions = [
            ["content-length-range", 1, request.max_file_size],
            {"Content-Type": request.file_type.value},
        ]

        presigned_post = s3_client.generate_presigned_post(
            Bucket=AWS_BUCKET_NAME,
            Key=file_key,
            Fields={"Content-Type": request.file_type.value},
            Conditions=conditions,
            ExpiresIn=request.expiration_seconds,
        )

        return PresignedURLResponse(
            upload_url=presigned_post["url"],
            form_fields=presigned_post["fields"],
            file_key=file_key,
            expires_in=request.expiration_seconds,
            max_file_size=request.max_file_size,
            content_type=request.file_type.value,
            bucket=AWS_BUCKET_NAME,
        )

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        raise HTTPException(
            status_code=500, detail=f"Failed to generate presigned URL: {error_code}"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


@app.post("/upload/confirm")
async def confirm_upload(file_key: str, user_id: str):
    """
    Confirm that a file was successfully uploaded to S3

    This endpoint verifies the file exists and can trigger post-upload processing
    """
    try:
        # Verify file exists in S3
        response = s3_client.head_object(Bucket=AWS_BUCKET_NAME, Key=file_key)

        file_size = response.get("ContentLength", 0)
        last_modified = response.get("LastModified")
        content_type = response.get("ContentType")

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
                "last_modified": last_modified.isoformat() if last_modified else None,
            },
        }

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            raise HTTPException(status_code=404, detail="File not found in S3")
        else:
            raise HTTPException(status_code=500, detail=f"S3 error: {error_code}")



@app.get("/poll-sqs")
async def poll_sqs():
    return await poll_sqs_messages()
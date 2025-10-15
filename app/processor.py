import random
import string
import os

from celery import Celery

from app.client import s3_client
from app.ducky.duckdb_importer import ParquetImporter


# Celery setup
celery_app = Celery(
    'file_processor',
    broker='redis://localhost:6379',
    backend='redis://localhost:6379',
    include=['app.processor', 'app.main']
)



@celery_app.task
def process_uploaded_file(bucket_name: str, object_key: str):
    """
    Process file uploaded to S3
    """


    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body']

    importer = ParquetImporter()

    try:
        length = 8
        db_file = ''.join(random.choices(string.ascii_letters + string.digits, k=length))

        importer.xml_path = file_content
        importer.path = db_file
        importer.export_xml()
    except:
        os.remove(db_file)


    uid = object_key.split('/')[0]
    filename = object_key.split('/')[-1]
    full_name = uid + '/processed/' + filename.replace('.xml', '.duckdb')

    s3_client.upload_file(db_file, bucket_name, full_name)

    os.remove(db_file)

    result = {
        "bucket": bucket_name,
        "key": object_key,
        "new_filename": full_name,
        "status": "processed"
    }

    return result


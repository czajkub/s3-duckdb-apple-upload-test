import os
import asyncio

import requests
from dotenv import load_dotenv

from app.main import create_presigned_upload_url
from app.schema import PresignedURLRequest


load_dotenv()

def main():
    # pre_url = requests.post(
    #     "http://localhost:8000/upload/presigned-url",
    #     headers={"Content-Type": "application/xml", "accept": "application/xml"},
    #     data=json.dumps({
    #         "user_id": "1",
    #           "file_type": "application/xml",
    #           "filename": "llmprep.xml",
    #           "expiration_seconds": 300,
    #           "max_file_size": 52428800
    #     })
    # ).json()
    xml_path = os.getenv("xml_path", "llmprep.xml")


    pre_url = asyncio.run(create_presigned_upload_url(PresignedURLRequest(
        user_id="1",
        file_type="application/xml",
        filename=f"{xml_path}",
    )))

    # print(pre_url)

    with open(xml_path, 'rb') as f:
        content = f.read()
        response = requests.post(
            pre_url.upload_url,
            data=pre_url.form_fields,
            files={'file': content}
        )
        print(response.status_code)
        print(response)

if __name__ == '__main__':
    main()
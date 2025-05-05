from dagster import asset, AssetExecutionContext, Definitions, define_asset_job, ScheduleDefinition
import requests
import pandas as pd
import io
import boto3
from datetime import datetime

# Garage S3 configuration
S3_ENDPOINT = 'http://localhost:3900'
S3_ACCESS_KEY = "GK904ad8d9d4e12205f574d8bb"
S3_SECRET_KEY = "9d9ec2f36a1787ba56675056312ac63719ef48fd198471bbcc722c036fcdae75"
S3_BUCKET = 'traffy-troffi'
S3_PREFIX = 'traffy-csvs'

@asset
def drive_csv_to_s3(context: AssetExecutionContext):
    """Download public CSV file from Google Drive and save it to Garage S3."""
    # Configuration
    file_id = "19QkF8i1my99gjbyHe7de_qZNwgrca6R5"  # The ID from the Drive URL
    # Create timestamp for the filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Log the process
    context.log.info(f"Starting download of Google Drive CSV at {timestamp}")

    # Direct download URL for public files
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

    # Set up S3 client
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    # Generate S3 key (path)
    s3_key = f"{S3_PREFIX}drive_file_{timestamp}.csv"

    # Initialize a session for better connection management
    session = requests.Session()

    # First request to handle any redirects and get cookies for large files
    response = session.get(download_url, stream=True)

    # Check if we need to confirm download for large files
    if 'download_warning' in response.url:
        # Extract the confirmation token
        token = response.url.split('id=')[1].split('&')[0]
        confirm_url = f"https://drive.google.com/uc?export=download&confirm=t&id={file_id}"
        response = session.get(confirm_url, stream=True)

    # Check if the download was successful
    if response.status_code != 200:
        error_msg = f"Failed to download file: HTTP {response.status_code}"
        context.log.error(error_msg)
        raise Exception(error_msg)

    # Get content length if available
    content_length = response.headers.get('content-length')
    if content_length:
        total_size = int(content_length)
        context.log.info(f"Downloading file of size: {total_size} bytes")

    try:
        # Stream the file directly to S3 using multipart upload
        mp_upload = s3_client.create_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3_key
        )

        # Process in chunks to handle large files
        parts = []
        part_number = 1
        chunk_size = 5 * 1024 * 1024  # 5MB chunks, minimum for S3 multipart
        buffer = io.BytesIO()
        bytes_transferred = 0

        for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1MB reading chunks
            if chunk:
                buffer.write(chunk)
                buffer.flush()
                bytes_transferred += len(chunk)

                # Log progress periodically
                if content_length and bytes_transferred % (10 * 1024 * 1024) < len(chunk):  # Log every ~10MB
                    progress = (bytes_transferred / total_size) * 100
                    context.log.info(f"Download progress: {progress:.1f}%")

                # When we've accumulated enough data, upload a part
                if buffer.tell() >= chunk_size:
                    buffer.seek(0)
                    # Upload part
                    part = s3_client.upload_part(
                        Bucket=S3_BUCKET,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=mp_upload['UploadId'],
                        Body=buffer.read(chunk_size)
                    )
                    parts.append({
                        'PartNumber': part_number,
                        'ETag': part['ETag']
                    })
                    part_number += 1

                    # Keep any remaining data in the buffer
                    remaining_data = buffer.read()
                    buffer = io.BytesIO()
                    buffer.write(remaining_data)

        # Upload any remaining data as the final part
        if buffer.tell() > 0:
            buffer.seek(0)
            part = s3_client.upload_part(
                Bucket=S3_BUCKET,
                Key=s3_key,
                PartNumber=part_number,
                UploadId=mp_upload['UploadId'],
                Body=buffer.read()
            )
            parts.append({
                'PartNumber': part_number,
                'ETag': part['ETag']
            })

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=S3_BUCKET,
            Key=s3_key,
            UploadId=mp_upload['UploadId'],
            MultipartUpload={'Parts': parts}
        )

        context.log.info(f"Successfully uploaded {bytes_transferred} bytes to {S3_BUCKET}/{s3_key}")

        # Return metadata without loading the entire file into memory
        return {
            "s3_bucket": S3_BUCKET,
            "s3_key": s3_key,
            "file_size_bytes": bytes_transferred,
            "timestamp": timestamp
        }

    except Exception as e:
        # Abort the multipart upload if something goes wrong
        context.log.error(f"Error during transfer: {str(e)}")
        try:
            s3_client.abort_multipart_upload(
                Bucket=S3_BUCKET,
                Key=s3_key,
                UploadId=mp_upload['UploadId']
            )
        except Exception as abort_error:
            context.log.error(f"Error aborting multipart upload: {str(abort_error)}")
        raise e

# Define a job to run this asset
download_job = define_asset_job("download_csv_to_s3_job", selection=[drive_csv_to_s3])

# Define a schedule
download_schedule = ScheduleDefinition(
    job=download_job,
    cron_schedule="* * * * *"
)

# Define the Dagster definitions
defs = Definitions(
    assets=[drive_csv_to_s3],
    schedules=[download_schedule]
)
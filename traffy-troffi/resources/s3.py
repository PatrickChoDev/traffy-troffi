import io
import json
import logging
from typing import Dict, Any

import boto3
from dagster import (
    ConfigurableResource,
    EnvVar
)

import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Resource(ConfigurableResource):
    """Resource for S3 operations"""
    endpoint_url: str = EnvVar("S3_ENDPOINT")
    access_key: str = EnvVar("S3_ACCESS_KEY")
    secret_key: str = EnvVar("S3_SECRET_KEY")
    bucket_name: str = EnvVar("S3_BUCKET_NAME")

    def get_client(self):
        """Get configured S3 client"""
        return boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def upload_json(self, key: str, data: Dict[str, Any]) -> Dict[str, str]:
        """Upload JSON resources to S3"""
        logger.info(f"Uploading resources to {self.bucket_name}/{key}")
        try:
            s3_client = self.get_client()

            # Convert resources to JSON string
            json_data = json.dumps(data, ensure_ascii=False)

            # Upload to S3
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
            )

            return {
                "bucket": self.bucket_name,
                "key": key
            }
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            raise
    def upload_csv(self, key: str, df: pd.DataFrame) -> Dict[str, str]:
        """Upload CSV data to S3"""
        logger.info(f"Uploading CSV to {self.bucket_name}/{key}")
        try:
            s3_client = self.get_client()
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            return {"bucket": self.bucket_name, "key": key}
        except Exception as e:
            logger.error(f"Error uploading CSV to S3: {e}")
            raise

    def upload_parquet(self, key: str, df: pd.DataFrame) -> Dict[str, str]:
        """Upload Parquet data to S3"""
        logger.info(f"Uploading Parquet to {self.bucket_name}/{key}")
        try:
            s3_client = self.get_client()
            parquet_buffer = io.BytesIO()

            df.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )

            return {"bucket": self.bucket_name, "key": key}
        except Exception as e:
            logger.error(f"Error uploading Parquet to S3: {e}")

    def upload_file(self, file: io.BytesIO, filename: str, content_type: str) -> Dict[str, str]:
        """Upload a file to S3"""
        logger.info(f"Uploading file to {self.bucket_name}/{filename}")
        try:
            s3_client = self.get_client()

            # Upload to S3
            s3_client.put_object(
                Body=file,
                Bucket=self.bucket_name,
                Key=filename,
                ContentType=content_type,
            )

            return {
                "Bucket": self.bucket_name,
                "Key": filename
            }
        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            raise

    def download_file(self, key: str) -> io.BytesIO:
        """Download a file from S3"""
        logger.info(f"Downloading file from {self.bucket_name}/{key}")
        try:
            s3_client = self.get_client()
            response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
            return io.BytesIO(response['Body'].read())
        except Exception as e:
            logger.error(f"Error downloading file from S3: {e}")
            raise

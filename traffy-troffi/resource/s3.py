from dagster import (
    ConfigurableResource,
    EnvVar
)
import boto3
import json
import logging
from typing import Dict, Any

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
        """Upload JSON data to S3"""
        logger.info(f"Uploading data to {self.bucket_name}/{key}")
        try:
            s3_client = self.get_client()

            # Convert data to JSON string
            json_data = json.dumps(data, ensure_ascii=False)

            # Upload to S3
            s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )

            return {
                "bucket": self.bucket_name,
                "key": key
            }
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            raise
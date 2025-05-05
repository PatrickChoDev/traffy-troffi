from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    define_asset_job,
    ScheduleDefinition,
    ConfigurableResource,
    EnvVar
)
import requests
import boto3
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Resource classes
class ApiResource(ConfigurableResource):
    """Resource for API interactions"""
    base_url: str
    timeout: int = 30

    def fetch_data(self) -> Dict[str, Any]:
        """Fetch data from the API"""
        logger.info(f"Fetching data from {self.base_url}")
        try:
            response = requests.get(self.base_url, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching API data: {e}")
            raise

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

# Asset definition
@asset(required_resource_keys={"api", "s3"})
def traffy_geojson_to_s3(context: AssetExecutionContext) -> Dict[str, Any]:
    """
    Asset that fetches GeoJSON data from Traffy API and stores it in S3.

    This asset runs on a schedule to keep data up-to-date.
    """
    # Create timestamp for the filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Log start of process
    context.log.info(f"Starting Traffy GeoJSON data fetch at {timestamp}")

    # Fetch data from API
    api_data = context.resources.api.fetch_data()

    # Validate the data has expected structure
    if not isinstance(api_data, dict):
        raise ValueError(f"API returned unexpected data type: {type(api_data)}")

    # Check if there's any data
    context.log.info(f"Received {len(json.dumps(api_data))} bytes of GeoJSON data")

    # Generate S3 key with timestamp
    s3_key = f"traffy/geojson/traffy_geojson_{timestamp}.json"

    # Upload to S3
    upload_result = context.resources.s3.upload_json(s3_key, api_data)

    # Also upload to a "latest" version
    latest_key = "traffy/geojson/latest.json"
    context.resources.s3.upload_json(latest_key, api_data)

    # Return metadata about the asset
    return {
        "s3_location": upload_result,
        "timestamp": timestamp,
        "data_size_bytes": len(json.dumps(api_data)),
        "features_count": len(api_data.get("features", [])) if "features" in api_data else None
    }

# Define resources
resources = {
    "api": ApiResource(
        base_url="https://publicapi.traffy.in.th/teamchadchart-stat-api/geojson/v1"
    ),
    "s3": S3Resource()
}

# Define a job
traffy_data_job = define_asset_job(
    "traffy_geojson_job",
    selection=[traffy_geojson_to_s3]
)

# Define a schedule - every 5 minutes
traffy_schedule = ScheduleDefinition(
    job=traffy_data_job,
    cron_schedule="*/5 * * * *"  # Every 5 minutes
)

# Define Dagster definitions
defs = Definitions(
    assets=[traffy_geojson_to_s3],
    resources=resources,
    schedules=[traffy_schedule],
    jobs=[traffy_data_job]
)
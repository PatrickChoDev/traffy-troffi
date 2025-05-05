from dagster import (
asset,
AssetExecutionContext,
ScheduleDefinition,
define_asset_job
)
from datetime import datetime
from ..resource.s3 import S3Resource
from ..data.traffy_geojson import ApiResource
import json
from typing import Dict, Any

# Asset definition
@asset(required_resource_keys={"traffy_api", "s3"})
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
    api_data = context.resources.traffy_api.fetch_data()

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

traffy_data_job = define_asset_job(
    "traffy_geojson_job",
    selection=[traffy_geojson_to_s3]
)

traffy_schedule = ScheduleDefinition(
    job=traffy_data_job,
    cron_schedule="*/5 * * * *"  # Every 5 minutes
)